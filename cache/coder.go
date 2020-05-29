package cache

import (
	"bytes"
	"encoding/binary"
)

func encode(value cacheValue) [] byte{

	e := value.Expire
	d := value.Data
	l := int32(len(d))
	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.BigEndian, e)
	binary.Write(bytesBuffer, binary.BigEndian, l)
	binary.Write(bytesBuffer, binary.BigEndian, d)

	return bytesBuffer.Bytes()
}

func decode(b [] byte) cacheValue{

	c := cacheValue{}
	var dataLen int32 = 0

	bytesBuffer := bytes.NewBuffer(b)
	binary.Read(bytesBuffer, binary.BigEndian, &c.Expire)
	binary.Read(bytesBuffer, binary.BigEndian, &dataLen)

	data := make([]byte, dataLen)
	binary.Read(bytesBuffer, binary.BigEndian, &data)
	c.Data = data

	return c
}



