package cmpp

import (
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/yedamao/go_cmpp/cmpp/conn"
	"github.com/yedamao/go_cmpp/cmpp/protocol"
)

type DeliverHandler func(dlv *protocol.Deliver)

type CmppConf struct {
	Addr         string
	SourceAddr   string
	SharedSecret string

	ServiceId string
	SrcId     string
}

func NewCmpp(name string, c CmppConf, deliverHandler DeliverHandler) (*Cmpp, error) {
	var idx uint32
	seqFn := func() uint32 {
		atomic.AddUint32(&idx, 1)
		return idx
	}

	cmppConn, err := conn.NewCmppConn(c.Addr, c.SourceAddr, c.SharedSecret, seqFn)
	if err != nil {
		return nil, err
	}

	return &Cmpp{
		name:           name,
		cc:             cmppConn,
		newSeqNum:      seqFn,
		deliverHandler: deliverHandler,
		conf:           c,
	}, nil
}

// Cmpp 客户端
// 在 CmppConn上增加
// 1. 自动keepalive
// 2. 断线重连
// 3. Deliver处理
// 4. 同步发送长短信
type Cmpp struct {
	sync.RWMutex

	name string

	cc             *conn.CmppConn
	newSeqNum      conn.SequenceFunc
	available      bool
	deliverHandler DeliverHandler // 连接Deliver处理

	checkJobOnce sync.Once
	done         chan struct{} // done for exit cmpp

	conf CmppConf
}

type SubmitLongtextResp struct {
	Total int
	Data  []Segment
}

type Segment struct {
	MsgId   uint64 // MsgId == 0 发送失败
	Result  uint8
	Content string
}

func (c *Cmpp) SubmitLongtext(phone, content, serviceID, srcID string) (*SubmitLongtextResp, error) {
	ltm := protocol.SplitLongText(content)

	ret := &SubmitLongtextResp{
		Total: int(ltm.Total),
		Data:  make([]Segment, 0, ltm.Total),
	}

	if serviceID == "" {
		serviceID = c.conf.ServiceId
	}
	if srcID == "" {
		srcID = c.conf.SrcId
	}
	for _, pdu := range ltm.Pdus {
		rp, err := c.syncSubmit(ltm.Total, pdu.No, 1, 0, serviceID, 0, "", protocol.UCS2,
			"02", "", srcID, []string{phone}, pdu.Data)
		if err != nil {
			log.Errorf("submit err: %v", err)
			return nil, err
		} else if rp != nil {
			ret.Data = append(ret.Data, Segment{
				Content: pdu.Content,
				MsgId:   rp.MsgId,
				Result:  rp.Result,
			})
		} else {
			ret.Data = append(ret.Data, Segment{
				Content: pdu.Content,
			})
		}
	}

	return ret, nil
}

func (c *Cmpp) syncSubmit(pkTotal, pkNumber, needReport, msgLevel uint8,
	serviceId string, feeUserType uint8, feeTerminalId string,
	msgFmt uint8, feeType, feeCode, srcId string,
	destTermId []string, content []byte) (*protocol.SubmitResp, error) {

	resChan := make(chan *protocol.SubmitResp, 1)
	handler := func(op protocol.Operation) {
		if op.GetHeader().Command_Id == protocol.CMPP_SUBMIT_RESP {
			if sr, ok := op.(*protocol.SubmitResp); ok {
				resChan <- sr
			}
		}
	}
	// default timeout
	go func() {
		time.Sleep(3 * time.Second)
		resChan <- nil
	}()

	c.cc.SubmitWithHandler(pkTotal, pkNumber, needReport, msgLevel,
		serviceId, feeUserType, feeTerminalId, msgFmt,
		feeType, feeCode, srcId, destTermId, content, handler)

	return <-resChan, nil

}

func (c *Cmpp) ResetConn() error {
	c.Lock()
	defer c.Unlock()

	cmppConn, err := conn.NewCmppConn(c.conf.Addr, c.conf.SourceAddr, c.conf.SharedSecret, c.newSeqNum)
	if err != nil {
		return err
	}
	c.cc = cmppConn
	return nil
}

// Close CmppConn and set available=false
func (c *Cmpp) Close() {
	log.Debugf("close cmpp:%v", c)
	c.Lock()
	defer c.Unlock()
	c.cc.Close()
	c.available = false
}

// Open CmppConn
func (c *Cmpp) Open() {
	c.Lock()
	c.available = true
	c.Unlock()

	c.checkJobOnce.Do(func() {
		go func() {
			ti := time.NewTicker(10 * time.Minute)
			for {
				select {
				case <-ti.C:
					if c.cc.Idle() > 3*time.Minute && c.IsAvailable() {
						log.Debugf("SP -> SMG ActiveTest")
						c.cc.ActiveTest()
					}
				case <-c.done:
					return
				}
			}
		}()
	})

	// handle response
	go func() {
		for {
			op, err := c.cc.Read() // This is blocking
			if err != nil {
				log.Errorf("fail to read, err:%v", err)
				c.Close()
				break
			}

			switch op.GetHeader().Command_Id {
			// handler逻辑已移到CmppConn
			case protocol.CMPP_SUBMIT_RESP:
				log.Debugf("ISMG -> SP CMPP_SUBMIT_RESP: %v", op)

			case protocol.CMPP_ACTIVE_TEST:
				log.Debugf("ISMG -> SP CMPP_ACTIVE_TEST: %v", op)
				c.cc.ActiveTestResp(op.GetHeader().Sequence_Id)

			case protocol.CMPP_ACTIVE_TEST_RESP:
				log.Debugf("ISMG -> SP CMPP_ACTIVE_TEST_RESP: %v", op)

			case protocol.CMPP_TERMINATE:
				log.Debugf("ISMG -> SP Terminate: %v", op)
				c.cc.TerminateResp(op.GetHeader().Sequence_Id)
				time.Sleep(3 * time.Second)
				c.Close()
				return

			case protocol.CMPP_TERMINATE_RESP:
				log.Debugf("ISMG -> SP Terminate Response: %v", op)
				time.Sleep(3 * time.Second)
				c.Close()
				return

			case protocol.CMPP_DELIVER:
				log.Debugf("ISMG -> SP Deliver: %v", op)
				dlv := op.(*protocol.Deliver)
				c.cc.DeliverResp(dlv.Header.Sequence_Id, dlv.MsgId, protocol.OK)
				if c.deliverHandler != nil {
					go c.deliverHandler(dlv)
				}
			default:
				log.Errorf("Unexpect CmdId: %0x", op.GetHeader().Command_Id)
			}
		}
	}()
}

// KeepOpen  cmpp连接断开自动重连
func (c *Cmpp) KeepOpen() {
	go func() {
		ti := time.NewTicker(3 * time.Second)
		for {
			select {
			case <-c.done:
				c.Close()
				return
			case <-ti.C:
				if !c.IsAvailable() {
					err := c.ResetConn()
					if err != nil {
						log.Errorf("fail to reset_conn, name:%v, err:%v", c.name, err)
					}
					c.Open()
				}
			}

		}
	}()
}

func (c *Cmpp) Exit() {
	c.done <- struct{}{}
}

func (c *Cmpp) IsAvailable() bool {
	c.RLock()
	defer c.RUnlock()
	return c.available
}
