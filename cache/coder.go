package cache

import (
	"bytes"
	"encoding/binary"
)

func encode(value value) [] byte{

	k := value.Key
	e := value.Expire
	d := value.Data
	kl := int32(len(k))
	vl := int32(len(d))

	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.BigEndian, e)
	binary.Write(bytesBuffer, binary.BigEndian, kl)

	key := []byte(k)
	binary.Write(bytesBuffer, binary.BigEndian, key)
	binary.Write(bytesBuffer, binary.BigEndian, vl)
	val := []byte(d)

	binary.Write(bytesBuffer, binary.BigEndian, val)

	return bytesBuffer.Bytes()
}

func decode(b [] byte) value {

	c := value{}
	var dataLen int32 = 0
	var keyLen int32 = 0

	bytesBuffer := bytes.NewBuffer(b)
	binary.Read(bytesBuffer, binary.BigEndian, &c.Expire)

	binary.Read(bytesBuffer, binary.BigEndian, &keyLen)
	key := make([]byte, keyLen)
	binary.Read(bytesBuffer, binary.BigEndian, &key)

	binary.Read(bytesBuffer, binary.BigEndian, &dataLen)
	data := make([]byte, dataLen)
	binary.Read(bytesBuffer, binary.BigEndian, &data)

	c.Key = string(key)
	c.Data = string(data)

	return c
}



