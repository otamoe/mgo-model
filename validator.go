package model

type (
	ValidatorInterface interface {
		ValidateDocument(document DocumentInterface) (err error)
	}
)

var Validator ValidatorInterface
