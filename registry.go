package arctic

import (
	"encoding/gob"
	"fmt"
	"reflect"
)

func RegisterGobType[MessageType any](sample MessageType) (err error) {
	var messageType reflect.Type = reflect.TypeOf(sample)

	if messageType == nil {
		messageType = typeOf[MessageType]()
	}

	if err = validateGobType(messageType); err != nil {
		return
	}

	if err = registerGobValue(sample); err != nil {
		return
	}

	gobTypes.add(messageType)
	return
}

func IsGobTypeRegistered[MessageType any]() (registered bool) {
	registered = gobTypes.has(typeOf[MessageType]())
	return
}

func requireGobTypeRegistered[MessageType any]() (err error) {
	if !IsGobTypeRegistered[MessageType]() {
		err = fmt.Errorf("%w: %s", ErrGobTypeNotRegistered, typeOf[MessageType]())
		return
	}

	return
}

func validateGobType(messageType reflect.Type) (err error) {
	if messageType.Kind() != reflect.Struct {
		err = fmt.Errorf("%w: %s", ErrGobTypeInvalid, messageType)
		return
	}

	return
}

func registerGobValue(sample any) (err error) {
	defer func() {
		var recovered any = recover()

		if recovered != nil {
			err = fmt.Errorf("%w: %v", ErrGobTypeRegistrationFailed, recovered)
		}
	}()

	gob.Register(sample)
	return
}

func typeOf[MessageType any]() (messageType reflect.Type) {
	messageType = reflect.TypeFor[MessageType]()
	return
}

func (registry *gobTypeRegistry) add(messageType reflect.Type) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()

	registry.registered[messageType] = struct{}{}
}

func (registry *gobTypeRegistry) has(messageType reflect.Type) (registered bool) {
	registry.mutex.RLock()
	defer registry.mutex.RUnlock()

	_, registered = registry.registered[messageType]
	return
}
