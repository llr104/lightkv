package test

import (
	"fmt"
	"lightkv/cache"
	"testing"
	"time"
)


func Test_Cache(t *testing.T)  {
	c := cache.NewCache(10)
	c.Put("lili", "hello", 10)
	c.Put("lili01", "hello lili01", 10)

	{
		v, ok := c.Get("lili")
		if ok{
			fmt.Printf("lili:%v\n", v)
		}else{
			fmt.Println("lili: not found")
		}
	}

	{
		v, ok := c.Get("lili01")
		if ok{
			fmt.Printf("lili01:%v\n", v)
		}else{
			fmt.Println("lili01: not found")
		}
	}

	c.Delete("lili01")
	fmt.Println("after delete lili01")

	{
		v, ok := c.Get("lili01")
		if ok{
			fmt.Printf("lili01:%v\n", v)
		}else{
			fmt.Println("lili01: not found")
		}
	}

	time.Sleep(12 * time.Second)
	fmt.Println("after 12 Seconds")

	{
		v, ok := c.Get("lili")
		if ok{
			fmt.Printf("lili:%v\n", v)
		}else{
			fmt.Println("lili: not found")
		}
	}

}
