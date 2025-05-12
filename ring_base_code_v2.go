// Código exemplo para o trabaho de sistemas distribuidos (eleicao em anel)
// By Cesar De Rose - 2022

package main

import (
	"fmt"
	"sync"
)

type mensagem struct {
	tipo  int    // tipo da mensagem para fazer o controle do que fazer (eleição, confirmacao da eleicao)
	corpo [3]int // conteudo da mensagem para colocar os ids (usar um tamanho ocmpativel com o numero de processos no anel)
}

var (
	chans = []chan mensagem{ // vetor de canias para formar o anel de eleicao - chan[0], chan[1] and chan[2] ...
		make(chan mensagem),
		make(chan mensagem),
		make(chan mensagem),
		make(chan mensagem),
	}
	controle = make(chan int)
	wg       sync.WaitGroup // wg is used to wait for the program to finish
)

// func ElectionControler(in chan int) {
// 	defer wg.Done()

// 	var temp mensagem

// 	// comandos para o anel iciam aqui

// 	temp.tipo = 5
// 	chans[0] <- temp
// 	fmt.Printf("Controle: confirmação %d\n", <-in)
// 	chans[1] <- temp
// 	fmt.Printf("Controle: Canal 0 e 1 falho\n")
// 	fmt.Printf("Controle: confirmação %d\n", <-in)

// 	temp.tipo = 1
// 	chans[2] <- temp
// 	fmt.Printf("Controle: Iniciando eleição\n")
// 	fmt.Printf("Controle: confirmação %d\n", <-in)

// 	temp.tipo = 6
// 	chans[0] <- temp
// 	chans[1] <- temp
// 	chans[2] <- temp
// 	chans[3] <- temp

// 	fmt.Println("\n   Processo controlador concluído\n")
// }

func ElectionControler(in chan int) {
	defer wg.Done()

	var temp mensagem

	// comandos para o anel iciam aqui

	temp.tipo = 5
	chans[0] <- temp
	fmt.Printf("Controle: confirmação %d\n", <-in)
	chans[1] <- temp
	fmt.Printf("Controle: confirmação %d\n", <-in)

	temp.tipo = 1
	chans[2] <- temp
	fmt.Printf("Controle: Iniciando eleição\n")
	fmt.Printf("Controle: confirmação %d\n", <-in)

	fmt.Printf("Controle: Revivendo 1\n")
	temp.tipo = 4
	chans[1] <- temp
	fmt.Printf("Controle: confirmação %d\n", <-in)

	fmt.Printf("Controle: Mata o processo líder\n")
	temp.tipo = 5
	chans[2] <- temp
	fmt.Printf("Controle: confirmação %d\n", <-in)

	temp.tipo = 4
	chans[0] <- temp
	fmt.Printf("Controle: Revive processo 0\n")
	fmt.Printf("Controle: confirmação %d\n", <-in)

	temp.tipo = 1
	chans[3] <- temp
	fmt.Printf("Controle: Iniciando eleição\n")
	fmt.Printf("Controle: confirmação %d\n", <-in)

	temp.tipo = 6
	chans[0] <- temp
	chans[1] <- temp
	chans[2] <- temp
	chans[3] <- temp

	fmt.Println("\n   Processo controlador concluído\n")
}

func ElectionStage(TaskId int, in chan mensagem, out chan mensagem, leader int) {
	defer wg.Done()

	var actualLeader int
	var bFailed bool = false
	var isElection bool = false
	var running bool = true

	actualLeader = leader

	for running {
		temp := <-in

		fmt.Printf("%2d: recebi mensagem %d, [ %d, %d, %d ]\n", TaskId, temp.tipo, temp.corpo[0], temp.corpo[1], temp.corpo[2])

		switch temp.tipo {
		/*
			1- Inicia eleição
			2- Votação
			3- Confirmação de líder
			4- Revive
			5- Morre
			6- Termina
		*/
		case 1:
			{
				isElection = true
				temp.corpo[0] = TaskId
				fmt.Printf("%2d: eleição iniciada: %v \n", TaskId, isElection)
				fmt.Printf("%2d: votei em %d\n", TaskId, temp.corpo[0])
				temp.tipo = 2
				out <- temp
				temp = <-in
				temp.tipo = 3
				fmt.Printf("%2d: Confirmando líder\n", TaskId)
				out <- temp
				temp = <-in
				fmt.Printf("%2d: Eleição concluída\n", TaskId)
				controle <- -5
			}
		case 2:
			{
				if !bFailed {
					isElection = true
					fmt.Printf("%2d: eleição iniciada: %v\n", TaskId, isElection)
					if temp.corpo[0] > TaskId {
						fmt.Printf("%2d: encontrei candidato melhor: %d\n", TaskId, TaskId)
						temp.corpo[0] = TaskId
					}
					fmt.Printf("%2d: votei em %d\n", TaskId, temp.corpo[0])
				}
				out <- temp
				// controle <- -5
			}
		case 3:
			{
				isElection = false
				actualLeader = temp.corpo[0]
				fmt.Printf("%2d: lider atual %d\n", TaskId, actualLeader)
				out <- temp
				// controle <- -5
			}
		case 4:
			{
				bFailed = false
				fmt.Printf("%2d: me reviveram\n", TaskId)
				fmt.Printf("%2d: falho %v \n", TaskId, bFailed)
				controle <- -5
			}
		case 5:
			{
				bFailed = true
				fmt.Printf("%2d: me mataram\n", TaskId)
				fmt.Printf("%2d: falho %v \n", TaskId, bFailed)
				controle <- -5
			}
		case 6:
			{
				running = false
				fmt.Printf("%2d: TERMINEI!!!!!!!!!\n", TaskId)
			}
		default:
			{
				fmt.Printf("%2d: não conheço este tipo de mensagem\n", TaskId)
				fmt.Printf("%2d: lider atual %d\n", TaskId, actualLeader)
			}
		}
	}
}

func main() {

	wg.Add(5) // Add a count of four, one for each goroutine

	// criar os processo do anel de eleicao

	go ElectionStage(3, chans[3], chans[0], 0) // este é o lider
	go ElectionStage(0, chans[0], chans[1], 0) // não é lider, é o processo 0
	go ElectionStage(1, chans[1], chans[2], 0) // não é lider, é o processo 0
	go ElectionStage(2, chans[2], chans[3], 0) // não é lider, é o processo 0

	fmt.Println("\n   Anel de processos criado")

	// criar o processo controlador

	go ElectionControler(controle)

	fmt.Println("\n   Processo controlador criado\n")

	wg.Wait() // Wait for the goroutines to finish\
}
