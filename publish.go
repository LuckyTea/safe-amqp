package amqp_safe

import "context"

func (c *Connector) Publish(exchange, key string, publishing Publishing) error {
	return c.PublishWithContext(context.Background(), exchange, key, publishing)
}

func (c *Connector) PublishWithContext(ctx context.Context, exchange, key string, publishing Publishing) error {
	c.wg.Add(1)
	defer c.wg.Done()

	c.pubmx.Lock()
	defer c.pubmx.Unlock()

	sch := c.ch
	if sch == nil {
		return ErrNoChannel
	}

	err := sch.PublishWithContext(ctx, exchange, key, true, false, publishing)
	if err != nil {
		return err
	}

	select {
	case ret := <-c.returns:
		c.cfg.Logger.Println("ERR [publish] message returned with err:", ret.ReplyText)

		ack := <-c.confirms
		if !ack.Ack {
			c.cfg.Logger.Println("INF [publish] message confirm nack after return")
			return ErrServerNAck
		}

		return ErrServerReturn
	case cnf := <-c.confirms:
		if !cnf.Ack {
			c.cfg.Logger.Println("INF [publish] message confirm nack")

			return ErrServerNAck
		}
	}

	return nil
}
