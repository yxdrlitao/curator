package main

import (
	"io"
	"log"
	"os"
	"path"
	"strings"
	"time"

	"github.com/yxdrlitao/curator"
	"github.com/yxdrlitao/go-zookeeper/zk"
)

func main() {
	filePath := "examples/base"
	// 日志
	log.SetFlags(log.Ldate | log.Lmicroseconds | log.Llongfile)
	file, err := os.OpenFile(path.Join(filePath, "out.log"), os.O_CREATE|os.O_TRUNC|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	log.SetOutput(io.MultiWriter(os.Stderr, file))

	builder := &curator.CuratorFrameworkBuilder{
		ConnectionTimeout: 2 * time.Second,
		SessionTimeout:    5 * time.Second,
		RetryPolicy:       curator.NewExponentialBackoffRetry(time.Second, 3, 15*time.Second),
	}

	curatorFramework := builder.ConnectString("127.0.0.1:2181").Build()
	err = curatorFramework.Start()
	if err != nil {
		log.Printf("start zookeeper client error:%v", err)
		return
	}

	nodePath := "/config/demo/1.0.0/DataSourceGroup"
	curatorFramework.CuratorListenable().AddListener(curator.NewCuratorListener(
		func(client curator.CuratorFramework, event curator.CuratorEvent) error {
			if event.Type() == curator.WATCHED {
				switch event.WatchedEvent().Type {
				case zk.EventNodeChildrenChanged:
					log.Println("EventNodeChildrenChanged")
					loadNode(curatorFramework, nodePath)
				case zk.EventNodeDataChanged:
					log.Println("EventNodeDataChanged")
					loadKey(curatorFramework, event.Path())
				default:
				}
			}
			return nil
		}))

	loadNode(curatorFramework, nodePath)

	select {}
}

func loadNode(curatorFramework curator.CuratorFramework, nodePath string) {
	childrenBuilder := curatorFramework.GetChildren()
	children, err := childrenBuilder.Watched().ForPath(nodePath)
	if err != nil {
		log.Println(err)
		return
	}

	for _, item := range children {
		loadKey(curatorFramework, path.Join(nodePath, item))
	}
}

func loadKey(curatorFramework curator.CuratorFramework, nodePath string) {
	data := curatorFramework.GetData()
	value, err := data.Watched().ForPath(nodePath)
	if err != nil {
		log.Printf("load property error: %s, %v", nodePath, err)
	} else {
		log.Printf("load property: %s, %v", getNodeFromPath(nodePath), string(value))
	}
}

func getNodeFromPath(path string) string {
	index := strings.LastIndex(path, `/`)
	if index < 0 {
		return path
	}

	if index+1 >= len(path) {
		return ""
	}

	return path[index+1:]
}
