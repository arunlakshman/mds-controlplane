// Package wire converts the contract value types to and from the generated api/v1
// messages. Nearly every function is generated (wire_gen.go, by ops/tools/wiregen
// from the .proto file); none of the conversions of a message is written by hand.
// A ToCore function reads the message with its Get methods, so a nil message or a
// missing part gives a zero value and never a panic. The one hand-written piece is
// the table of error codes (errcode.go), because the names of the two sides differ.
package wire
