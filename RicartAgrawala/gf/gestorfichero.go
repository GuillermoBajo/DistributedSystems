package gf

import (
	"os"
	"fmt"
)

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

type GestorFichero struct {
	nombreFichero string
}

// LeerFichero es un método que lee el contenido del archivo asociado al GestorFichero.
func (f *GestorFichero) LeerFichero() string {
	// Lee el contenido del archivo.
	txt, err := os.ReadFile(f.nombreFichero)
	checkError(err)
	
	return string(txt)
}

// EscribirFichero es un método que escribe un fragmento de texto en el archivo asociado al GestorFichero.
func (f *GestorFichero) EscribirFichero(fragmento string) error {
	// Abre el archivo en modo escritura, creándolo si no existe o sobrescribiéndolo si existe.
	file, err := os.OpenFile(f.nombreFichero, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	checkError(err)
	defer file.Close() // Asegúrate de cerrar el archivo al final de la función.

	// Escribe el fragmento en el archivo.
	_, err = file.WriteString(fragmento)
	checkError(err)

	return nil
}

// New es un método que crea un GestorFichero a partir del nombre de un fichero.
func New(nombreFichero string) (*GestorFichero) {
	_, err := os.Create(nombreFichero)
	checkError(err)
	
	gf := GestorFichero{nombreFichero}
	return &gf
}

