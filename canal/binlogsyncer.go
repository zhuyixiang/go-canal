package canal

import (
	. "github.com/zhuyixiang/go-canal/events"
	"github.com/zhuyixiang/go-canal/client"
	"golang.org/x/net/context"
	"github.com/ngaut/log"
	"github.com/juju/errors"
	"encoding/binary"
	"fmt"
	"os"
	"time"
)

var (
	ErrNeedSyncAgain = errors.New("Last sync error or closed, try sync and get event again")
	ErrSyncClosed = errors.New("Sync was closed")
)

type BinlogSyncer struct {
	cfg     *MysqlConfig
	c       *client.Conn
	parser  *BinlogParser
	nextPos Position
	running bool
	ctx     context.Context
	cancel  context.CancelFunc
	ech     chan *BinlogEvent
}

func NewBinlogSyncer(cfg *MysqlConfig) *BinlogSyncer {
	log.SetLevelByString("info")

	log.Infof("create BinlogSyncer with config %v", cfg)

	b := new(BinlogSyncer)

	b.cfg = cfg
	b.parser = NewBinlogParser()
	b.running = false
	b.ctx, b.cancel = context.WithCancel(context.Background())
	b.ech = make(chan *BinlogEvent, 10240)
	return b
}

func (b *BinlogSyncer) StartSync(pos Position) (error) {
	log.Infof("begin to sync binlog from position %s", pos)

	if err := b.prepareSyncPos(pos); err != nil {
		return errors.Trace(err)
	}

	b.running = true
	go b.dumpEvent()

	return nil
}

func (b *BinlogSyncer) prepareSyncPos(pos Position) error {
	if b.IsClosed() {
		return errors.Trace(ErrSyncClosed)
	}

	// always start from position 4
	if pos.Pos < 4 {
		pos.Pos = 4
	}

	if err := b.registerSlave(); err != nil {
		return errors.Trace(err)
	}

	if err := b.writeBinglogDumpCommand(pos); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (b *BinlogSyncer) dumpEvent() {
	defer func() {
		if e := recover(); e != nil {
			b.Close()
		}
	}()

	for {
		if (!b.running) {
			break
		}

		data, err := b.c.ReadPacket()
		if err != nil {
			log.Error(err)

			// we meet connection error, should re-connect again with
			// last nextPos we got.
			if len(b.nextPos.Name) == 0 {
				// we can't get the correct position, close.
				b.Close()
				return
			}

			// TODO: add a max retry count.
			for {
				select {
				case <-b.ctx.Done():
					b.Close()
					return
				case <-time.After(time.Second):
					if err = b.retrySync(); err != nil {
						log.Errorf("retry sync err: %v, wait 1s and retry again", err)
						continue
					}
				}

				break
			}

			// we connect the server and begin to re-sync again.
			continue
		}

		switch data[0] {
		case OK_HEADER:
			if err = b.parseEvent(data); err != nil {
				b.Close()
				return
			}
		case ERR_HEADER:
			err = b.c.HandleErrorPacket(data)
			b.Close()
			return
		case EOF_HEADER:
			// Refer http://dev.mysql.com/doc/internals/en/packet-EOF_Packet.html
			// In the MySQL client/server protocol, EOF and OK packets serve the same purpose.
			// Some users told me that they received EOF packet here, but I don't know why.
			// So we only log a message and retry ReadPacket.
			log.Info("receive EOF packet, retry ReadPacket")
			continue
		default:
			log.Errorf("invalid stream header %c", data[0])
			continue
		}
	}
}

func (b *BinlogSyncer) retrySync() error {
	log.Infof("begin to re-sync from %s", b.nextPos)

	b.parser.Reset()
	if err := b.prepareSyncPos(b.nextPos); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (b *BinlogSyncer) registerSlave() error {
	if b.c != nil {
		b.c.Close()
	}

	log.Infof("register slave for master server %s:%d", b.cfg.Host, b.cfg.Port)
	var err error
	b.c, err = client.Connect(fmt.Sprintf("%s:%d", b.cfg.Host, b.cfg.Port), b.cfg.User, b.cfg.Password, "", func(c *client.Conn) {

	})
	if err != nil {
		return errors.Trace(err)
	}

	//b.c.Execute("set @master_heartbeat_period=3000000000;");

	if len(b.cfg.Charset) != 0 {
		b.c.SetCharset(b.cfg.Charset)
	}


	//for mysql 5.6+, binlog has a crc32 checksum
	//before mysql 5.6, this will not work, don't matter.:-)
	if r, err := b.c.Execute("SHOW GLOBAL VARIABLES LIKE 'BINLOG_CHECKSUM'"); err != nil {
		return errors.Trace(err)
	} else {
		s, _ := r.GetString(0, 1)
		if s != "" {
			// maybe CRC32 or NONE

			// mysqlbinlog.cc use NONE, see its below comments:
			// Make a notice to the server that this client
			// is checksum-aware. It does not need the first fake Rotate
			// necessary checksummed.
			// That preference is specified below.

			if _, err = b.c.Execute(`SET @master_binlog_checksum='NONE'`); err != nil {
				return errors.Trace(err)
			}

			// if _, err = b.c.Execute(`SET @master_binlog_checksum=@@global.binlog_checksum`); err != nil {
			// 	return errors.Trace(err)
			// }

		}
	}

	if err = b.writeRegisterSlaveCommand(); err != nil {
		return errors.Trace(err)
	}

	if _, err = b.c.ReadOKPacket(); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (b *BinlogSyncer) writeRegisterSlaveCommand() error {
	b.c.ResetSequence()

	hostname := b.localHostname()

	// This should be the name of slave host not the host we are connecting to.
	data := make([]byte, 4 + 1 + 4 + 1 + len(hostname) + 1 + len(b.cfg.User) + 1 + len(b.cfg.Password) + 2 + 4 + 4)
	pos := 4

	data[pos] = COM_REGISTER_SLAVE
	pos++

	binary.LittleEndian.PutUint32(data[pos:], b.cfg.ServerID)
	pos += 4

	// This should be the name of slave hostname not the host we are connecting to.
	data[pos] = uint8(len(hostname))
	pos++
	n := copy(data[pos:], hostname)
	pos += n

	data[pos] = uint8(len(b.cfg.User))
	pos++
	n = copy(data[pos:], b.cfg.User)
	pos += n

	data[pos] = uint8(len(b.cfg.Password))
	pos++
	n = copy(data[pos:], b.cfg.Password)
	pos += n

	binary.LittleEndian.PutUint16(data[pos:], uint16(b.cfg.Port))
	pos += 2

	//replication rank, not used
	binary.LittleEndian.PutUint32(data[pos:], 0)
	pos += 4

	// master ID, 0 is OK
	binary.LittleEndian.PutUint32(data[pos:], 0)

	return b.c.WritePacket(data)
}

func (b *BinlogSyncer) writeBinglogDumpCommand(p Position) error {
	b.c.ResetSequence()

	data := make([]byte, 4 + 1 + 4 + 2 + 4 + len(p.Name))

	pos := 4
	data[pos] = COM_BINLOG_DUMP
	pos++

	binary.LittleEndian.PutUint32(data[pos:], p.Pos)
	pos += 4

	binary.LittleEndian.PutUint16(data[pos:], BINLOG_DUMP_NEVER_STOP)
	pos += 2

	binary.LittleEndian.PutUint32(data[pos:], b.cfg.ServerID)
	pos += 4

	copy(data[pos:], p.Name)

	return b.c.WritePacket(data)
}

func (b *BinlogSyncer) localHostname() string {
	if len(b.cfg.Localhost) == 0 {
		h, _ := os.Hostname()
		return h
	}
	return b.cfg.Localhost
}

func (b *BinlogSyncer) Close() {
	if b.IsClosed() {
		return
	}
	b.running = false
	b.cancel()
	if b.c != nil {
		b.c.Close()
	}
	log.Info("syncer is closed")
}

func (b *BinlogSyncer) IsClosed() bool {
	select {
	case <-b.ctx.Done():
		return true
	default:
		return false
	}
}

func (b *BinlogSyncer) GetEvent() (*BinlogEvent, error) {
	if !b.running {
		return nil, ErrSyncClosed
	}

	select {
	case c := <-b.ech:
		return c, nil
	case <-b.ctx.Done():
		return nil, b.ctx.Err()
	}
}

func (b *BinlogSyncer) parseEvent(data []byte) error {
	//skip OK byte, 0x00
	data = data[1:]

	e, err := b.parser.Parse(data)
	if err != nil {
		return errors.Trace(err)
	}

	if e.Header.LogPos > 0 {
		// Some events like FormatDescriptionEvent return 0, ignore.
		b.nextPos.Pos = e.Header.LogPos
	}

	if re, ok := e.Event.(*RotateEvent); ok {
		b.nextPos.Name = string(re.NextLogName)
		b.nextPos.Pos = uint32(re.Position)
		log.Infof("rotate to %s", b.nextPos)
	}

	needStop := false
	select {
	case b.ech <- e:
	case <-b.ctx.Done():
		needStop = true
	}

	if needStop {
		return errors.New("sync is been closing...")
	}

	return nil
}
