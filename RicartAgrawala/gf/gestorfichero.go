package gf

import (
	"os"
	"fmt"
)

// checkError checks if an error occurred and prints it to stderr before terminating the program.
func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

// GestorFichero is a struct that represents a file manager that handles file operations.
type GestorFichero struct {
	nombreFichero string // Name of the file to be managed.
}

// LeerFichero is a method that reads the content of the file associated with the GestorFichero.
func (f *GestorFichero) LeerFichero() string {
	// Reads the content of the file.
	txt, err := os.ReadFile(f.nombreFichero)
	checkError(err)
	
	return string(txt) // Returns the content of the file as a string.
}

// EscribirFichero is a method that writes a text fragment into the file associated with the GestorFichero.
func (f *GestorFichero) EscribirFichero(fragmento string) error {
	// Opens the file in write mode, creating it if it doesn't exist or overwriting it if it does.
	file, err := os.OpenFile(f.nombreFichero, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	checkError(err)
	defer file.Close() // Ensures the file is closed at the end of the function.

	// Writes the fragment into the file.
	_, err = file.WriteString(fragmento)
	checkError(err)

	return nil
}

// New is a method that creates a GestorFichero instance from the given file name.
func New(nombreFichero string) (*GestorFichero) {
	// Creates a new file if it doesn't exist and checks for any errors.
	_, err := os.Create(nombreFichero)
	checkError(err)
	
	// Returns a pointer to a new GestorFichero instance.
	gf := GestorFichero{nombreFichero}
	return &gf
}
