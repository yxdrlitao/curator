package main

import (
	"log"
	"time"

	"github.com/yxdrlitao/curator"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Llongfile)
	//_, err := os.OpenFile("out.log", os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	//if err != nil {
	//	panic(err)
	//}
	//log.SetOutput(file)

	builder := &curator.CuratorFrameworkBuilder{
		ConnectionTimeout: 1 * time.Second,
		SessionTimeout:    1 * time.Second,
		RetryPolicy:       curator.NewExponentialBackoffRetry(time.Second, 3, 15*time.Second),
	}

	curatorFramework := builder.ConnectString("127.0.0.1:2181").Build()
	err := curatorFramework.Start()
	if err != nil {
		log.Printf("start zookeeper client error:%v", err)
		return
	}

	//curatorFramework.CuratorListenable().AddListener(curator.NewCuratorListener(
	//	func(client curator.CuratorFramework, event curator.CuratorEvent) error {
	//		if event.Type() == curator.WATCHED {
	//			switch event.WatchedEvent().Type {
	//			case zk.EventNodeChildrenChanged:
	//				log.Println("EventNodeChildrenChanged")
	//			case zk.EventNodeDataChanged:
	//				log.Println("EventNodeDataChanged")
	//			default:
	//			}
	//		}
	//		return nil
	//	}))

	//childrenBuilder := curatorFramework.GetChildren()
	//children, err := childrenBuilder.Watched().ForPath("/config/demo/1.0.0/DataSourceGroup")
	//if err != nil {
	//	log.Println(err)
	//	return
	//}
	//fmt.Println(children)

	select {}
}
