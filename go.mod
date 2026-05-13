module github.com/lwdjd/IPFAR

go 1.25.3

require (
	github.com/LWDJD/ipfar-sdk v0.0.0-20260417020757-342c71e1df8e
	github.com/Xuanwo/go-locale v1.1.3
	github.com/leonelquinteros/gotext v1.7.2
	golang.org/x/text v0.36.0
)

require (
	golang.org/x/crypto v0.50.0 // indirect
	golang.org/x/sys v0.43.0 // indirect
)

replace github.com/LWDJD/ipfar-sdk => ../ipfar-sdk
