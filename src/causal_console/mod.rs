// Create a text area interface with all the methods of a terminal text area.
//
// When the text area is initiated a string is passed which will be manipulated based on key presses.
// The string will default to the content "|" where | is the cursor representation.
//
// For character removed or inserted an event is generated which is dispatched through a trait which will
// be implemented by a certain element.
//
// The whole text area will loop and clear the terminal screen for each printing and it will end with a given
// key combination.
//
// Useful libraries:
// https://crates.io/crates/console
// https://lib.rs/crates/clearscreen