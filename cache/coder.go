package cache

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
)

func encodeValue(value Value) [] byte{

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

func decodeValue(b [] byte) Value {

	c := Value{}
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

func encodeHM(value MapValue) [] byte{

	k := value.Key
	e := value.Expire
	d, _ := json.Marshal(value.Data)

	kl := int32(len(k))
	vl := int32(len(d))

	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.BigEndian, e)
	binary.Write(bytesBuffer, binary.BigEndian, kl)

	key := []byte(k)
	binary.Write(bytesBuffer, binary.BigEndian, key)
	binary.Write(bytesBuffer, binary.BigEndian, vl)
	val := d

	binary.Write(bytesBuffer, binary.BigEndian, val)

	return bytesBuffer.Bytes()
}

func decodeHM(b [] byte) MapValue {

	c := MapValue{}
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
	m := make(map[string]string)
	json.Unmarshal(data, &m)
	c.Data = m

	return c
}

func encodeList(value ListValue) [] byte{

	k := value.Key
	e := value.Expire
	d, _ := json.Marshal(value.Data)

	kl := int32(len(k))
	vl := int32(len(d))

	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.BigEndian, e)
	binary.Write(bytesBuffer, binary.BigEndian, kl)

	key := []byte(k)
	binary.Write(bytesBuffer, binary.BigEndian, key)
	binary.Write(bytesBuffer, binary.BigEndian, vl)
	val := d

	binary.Write(bytesBuffer, binary.BigEndian, val)

	return bytesBuffer.Bytes()
}

func decodeList(b [] byte) ListValue {

	c := ListValue{}
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
	json.Unmarshal(data, &c.Data)

	return c
}

func encodeSet(value SetValue) [] byte{

	k := value.Key
	e := value.Expire

	val, _ := json.Marshal(value.all())

	kl := int32(len(k))
	vl := int32(len(val))


	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.BigEndian, e)
	binary.Write(bytesBuffer, binary.BigEndian, kl)

	key := []byte(k)
	binary.Write(bytesBuffer, binary.BigEndian, key)
	binary.Write(bytesBuffer, binary.BigEndian, vl)
	binary.Write(bytesBuffer, binary.BigEndian, val)

	return bytesBuffer.Bytes()
}

func decodeSet(b [] byte) SetValue {

	c := SetValue{}
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
	var arr []string
	json.Unmarshal(data, &arr)
	m := make(map[string]string)
	for _,v:= range arr{
		m[v] = v
	}
	c.Data = m

	return c
}


