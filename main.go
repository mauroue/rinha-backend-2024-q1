package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"time"
	"unicode/utf8"

	"github.com/gofiber/fiber/v3"
	"github.com/jackc/pgx/v5/pgxpool"
)

var dbpool *pgxpool.Pool

func main() {
	app := fiber.New()
	var err error

	dsn := fmt.Sprintf("host=%s user=%s dbname=%s password=%s sslmode=disable",
		os.Getenv("POSTGRES_HOST"),
		os.Getenv("POSTGRES_USER"),
		os.Getenv("POSTGRES_DB"),
		os.Getenv("POSTGRES_PASSWORD"))

	dbpool, err = pgxpool.New(context.Background(), dsn)

	if err != nil {
		log.Fatal("Error creating pool: ", err)
	}

	err = dbpool.Ping(context.Background())
	if err != nil {
		log.Fatal("Error pinging database: ", err)
	}

	app.Get("/clientes/:id/extrato", handleTransactionLog)
	app.Post("/clientes/:id/transacoes", handleTransactions)

	log.Fatal(app.Listen(":8080"))
}

func clientExists(id int) error {
	if id > 0 && id <= 5 {
		return nil
	}
	return errors.New("Cliente não existe.")
}

func handleTransactions(c fiber.Ctx) error {

	clientId, err := c.ParamsInt("id")
	err = clientExists(clientId)
	if err != nil {
		return c.SendStatus(fiber.StatusNotFound)
	}

	transaction := new(TransacaoRequest)

	if err := json.Unmarshal(c.Body(), &transaction); err != nil {
		return c.SendStatus(fiber.ErrUnprocessableEntity.Code)
	}

	var length = utf8.RuneCountInString(transaction.Descricao)
	if length > 10 || length < 1 {
		return c.SendStatus(fiber.ErrUnprocessableEntity.Code)
	}

	if transaction.Tipo != "c" && transaction.Tipo != "d" {
		return c.SendStatus(fiber.ErrUnprocessableEntity.Code)
	}
	_, err = dbpool.Exec(context.Background(), `
		INSERT INTO transacoes 
		(valor, tipo, descricao, cliente_id) 
		VALUES ($1, $2, $3, $4)
		`,
		transaction.Valor,
		transaction.Tipo,
		transaction.Descricao,
		clientId)

	if err != nil {
		return c.SendStatus(fiber.ErrUnprocessableEntity.Code)
	}

	var response Balance

	row := dbpool.QueryRow(context.Background(), "SELECT limite, saldo from clientes where id = $1", clientId)
	row.Scan(&response.Limite, &response.Saldo)

	jsonResponse, err := json.Marshal(response)

	c.Response().Header.Set("Content-Type", "application/json")
	c.Response().SetBody(jsonResponse)

	return nil
}

func handleTransactionLog(c fiber.Ctx) error {
	clientId, err := c.ParamsInt("id")

	err = clientExists(clientId)
	if err != nil {
		return c.SendStatus(fiber.StatusNotFound)
	}

	var transactions []Transacao
	rows, err := dbpool.Query(context.Background(), `
		SELECT valor, tipo, descricao, realizada_em 
		FROM transacoes WHERE cliente_id = $1 
		ORDER BY realizada_em DESC LIMIT 10`, clientId)
	if err != nil {
		return c.SendStatus(fiber.ErrUnprocessableEntity.Code)
	}

	var balance BalanceResponse
	err = dbpool.QueryRow(context.Background(), `
		SELECT saldo, limite FROM clientes WHERE ID = $1`,
		clientId).Scan(&balance.Total, &balance.Limite)
	if err != nil {
		return c.SendStatus(fiber.ErrInternalServerError.Code)
	}
	for rows.Next() {
		var transaction Transacao
		err = rows.Scan(
			&transaction.Valor,
			&transaction.Tipo,
			&transaction.Descricao,
			&transaction.RealizadaEm,
		)
		if err != nil {
			return c.SendStatus(fiber.ErrBadRequest.Code)
		}
		transactions = append(transactions, transaction)
	}
	finalResponse := TransactionLog{
		Saldo: BalanceResponse{
			Total:       balance.Total,
			Limite:      balance.Limite,
			DataExtrato: time.Now().UTC(),
		},
		UltimasTransacoes: transactions,
	}
	if err != nil {
		return c.SendStatus(fiber.ErrInternalServerError.Code)
	}

	jsonResponse, err := json.Marshal(finalResponse)
	if err != nil {
		return c.SendStatus(fiber.ErrInternalServerError.Code)
	}

	c.Response().Header.Set("Content-Type", "application/json")
	c.Response().SetBody(jsonResponse)
	return nil
}

// Cliente representa a estrutura de dados de um cliente
type Cliente struct {
	ID         int         `json:"id"`
	Limite     int         `json:"limite"`
	Saldo      int         `json:"saldo"`
	Transacoes []Transacao `json:"transacoes"`
}

// Transacao representa a estrutura de dados de uma transação
type Transacao struct {
	Valor       int       `json:"valor"`
	Tipo        string    `json:"tipo"`
	Descricao   string    `json:"descricao"`
	RealizadaEm time.Time `json:"realizada_em"`
}

// TransacaoRequest representa a estrutura de dados de uma requisicao de transação
type TransacaoRequest struct {
	Valor     int    `json:"valor"`
	Tipo      string `json:"tipo"`
	Descricao string `json:"descricao"`
}

type Balance struct {
	Saldo  int `json:"saldo"`
	Limite int `json:"limite"`
}

// ExtratoResponse representa a estrutura de dados da resposta do endpoint /clientes/[id]/extrato
type TransactionLog struct {
	Saldo             BalanceResponse `json:"saldo"`
	UltimasTransacoes []Transacao     `json:"ultimas_transacoes"`
}

// SaldoResponse representa a estrutura de dados do saldo na resposta do extrato
type BalanceResponse struct {
	Total       int       `json:"total"`
	DataExtrato time.Time `json:"data_extrato"`
	Limite      int       `json:"limite"`
}
