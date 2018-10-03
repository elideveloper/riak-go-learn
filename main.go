package main

import (
	"encoding/json"
	"fmt"

	"github.com/tpjg/goriakpbc"
	//riak "github.com/basho/riak-go-client"
)

func main() {
	/*
		nodeOpts := &riak.NodeOptions{
			RemoteAddress: "127.0.0.1:8087",
		}

		var node *riak.Node
		var err error
		if node, err = riak.NewNode(nodeOpts); err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}

		nodes := []*riak.Node{node}
		opts := &riak.ClusterOptions{
			Nodes: nodes,
		}

		cluster, err := riak.NewCluster(opts)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}

		defer func() {
			if err := cluster.Stop(); err != nil {
				fmt.Println(err.Error())
				os.Exit(1)
			}
		}()

		if err := cluster.Start(); err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}

		obj := &riak.Object{
			ContentType:     "text/plain",
			Charset:         "utf-8",
			ContentEncoding: "utf-8",
			Value:           []byte("уууууууууууууууу"),
		}

		cmd, err := riak.NewStoreValueCommandBuilder().
			WithBucket("testBucketName").
			WithContent(obj).
			Build()
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}

		if err := cluster.Execute(cmd); err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}

		svc := cmd.(*riak.StoreValueCommand)
		rsp := svc.Response
		fmt.Println(rsp.GeneratedKey)

		// --------------------------------
		o := &riak.NewClientOptions{
			RemoteAddresses: []string{"127.0.0.1:8087"},
		}
		var c *riak.Client
		c, err = riak.NewClient(o)
		if err != nil {
			os.Exit(1)
		}

		cmds := make([]riak.Command, 0)

		// fetch customer
		cmd, err = riak.NewFetchValueCommandBuilder().
			WithBucket("testBucketName").
			WithKey(rsp.GeneratedKey).
			Build()
		if err != nil {
			os.Exit(1)
		}
		cmds = append(cmds, cmd)

		doneChan := make(chan riak.Command)
		errored := false
		for _, cmd := range cmds {
			a := &riak.Async{
				Command: cmd,
				Done:    doneChan,
			}
			if eerr := c.ExecuteAsync(a); eerr != nil {
				errored = true
				fmt.Println(eerr)
			}
		}
		if errored {
			os.Exit(1)
		}

		for i := 0; i < len(cmds); i++ {
			select {
			case d := <-doneChan:
				if fv, ok := d.(*riak.FetchValueCommand); ok {
					obj := fv.Response.Values[0]
					switch obj.Bucket {
					case "testBucketName":
						fmt.Println("Customer     1: %v", string(obj.Value))
					}
				} else {
					os.Exit(1)
				}
			case <-time.After(5 * time.Second):
				os.Exit(1)
			}
		}
	*/

	err := riak.ConnectClientPool("127.0.0.1:8087", 2)
	if err != nil {
		fmt.Println("Cannot connect, is Riak running?")
		return
	}

	defer riak.Close()

	bucket, _ := riak.NewBucket("buck")
	obj := bucket.NewObject("val")
	obj.ContentType = "application/json"
	obj.Data = []byte(`{"field":"value"}`)
	obj.Store()

	var dat map[string]interface{}

	fmt.Printf("Stored an object in Riak, vclock = %v\n", obj.Vclock)
	fmt.Printf("Stored an object in Riak, data = %v\n", obj.Data)
	/*
		rObj, err := riak.GetFrom("tstriak", "tstobj")
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Println(fmt.Sprintf("%s", rObj.Data))

		var dat map[string]interface{}

		if err := json.Unmarshal(rObj.Data, &dat); err != nil {
			panic(err)
		}
		fmt.Println(dat)*/

	cl := riak.New("127.0.0.1:8087")
	buck, err := cl.Bucket("buck")
	if err != nil {
		fmt.Println(err)
		return
	}
	rObj, err := buck.Get("val")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(fmt.Sprintf("%s", rObj.Data))

	if err := json.Unmarshal(rObj.Data, &dat); err != nil {
		panic(err)
	}
	fmt.Println(dat)
}
