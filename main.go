package main

import (
	"bufio"
	"bytes"
	"database/sql"
	"fmt"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"unicode"

	_ "github.com/lib/pq"
)

// cosntantes para acesso à instância do PostgreSQL e o banco de dados
const (
	HOST     = "localhost"
	PORT     = 5432
	DATABASE = "NEOWAY"
	USER     = "postgres"
	PASSWORD = "postgres"
	BITSIZE  = 64
)

// variáveis para uso comum
var (
	CPFRegexp  = regexp.MustCompile(`^\d{3}\.?\d{3}\.?\d{3}-?\d{2}$`)
	CNPJRegexp = regexp.MustCompile(`^\d{2}\.?\d{3}\.?\d{3}\/?(:?\d{3}[1-9]|\d{2}[1-9]\d|\d[1-9]\d{2}|[1-9]\d{3})-?\d{2}$`)
)

// função para tratar erros de maneira genérica
func registrarErro(aErro error) {
	if aErro != nil {
		log.Fatal("ERRO: ", aErro)
	}
}

// validar se o CPF é valido
func validarCPF(aCpf string) bool {
	const (
		TAMANHO = 11
		POSICAO = 10
	)

	return ehCpfOuCnpj(aCpf, CPFRegexp, TAMANHO, POSICAO)
}

// validar se o CNPJ é valido
func validarCNPJ(aCnpj string) bool {
	const (
		TAMANHO = 14
		POSICAO = 5
	)

	return ehCpfOuCnpj(aCnpj, CNPJRegexp, TAMANHO, POSICAO)
}

func ehCpfOuCnpj(aDocumento string, aRegra *regexp.Regexp, aTamanho int, aPosicao int) bool {
	if !aRegra.MatchString(aDocumento) {
		return false
	}

	limparCaracteresCpfCnpj(&aDocumento)

	documento := aDocumento[:aTamanho]
	digitoVerificador := calcularDigitoVerificador(documento, aPosicao+1)

	return aDocumento == documento+digitoVerificador
}

// eliminar caracteres especiais para CPF e CNPJ
func limparCaracteresCpfCnpj(aDocumento *string) {
	buf := bytes.NewBufferString("")

	for _, digito := range *aDocumento {
		if unicode.IsDigit(digito) {
			buf.WriteRune(digito)
		}
	}

	*aDocumento = buf.String()
}

func toInt(aValue rune) int {
	return int(aValue - '0')
}

// verificar se CPF CNPJ validos
func calcularDigitoVerificador(aDocumento string, aPosicao int) string {
	var soma int

	for _, digito := range aDocumento {
		soma += toInt(digito) * aPosicao
		aPosicao--

		if aPosicao < 2 {
			aPosicao = 9
		}
	}

	soma %= 11

	if soma < 2 {
		return "0"
	}

	return strconv.Itoa(11 - soma)
}

func main() {
	// string para conexão com o banco de dados, utilizando as constantes para parametrizar
	var stringConexao string = fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		HOST, PORT, USER, PASSWORD, DATABASE)

	// conectar no banco de dados e recuperar possível erro de conexão
	db, erro := sql.Open("postgres", stringConexao)
	registrarErro(erro)

	// evitar que a conexão se feche após o uso, manter uma instância durante execução do programa
	defer db.Close()

	// verificar se a conexão é válida
	erro = db.Ping()
	registrarErro(erro)

	fmt.Println("Conexão efetuada com sucesso!")

	// query da tabela PESSOA no banco de dados
	rows, erro := db.Query("SELECT COUNT(ID_PESSOA) AS TOTAL_REGISTROS FROM PESSOA;")
	registrarErro(erro)
	defer rows.Close()

	var totalRegistros int = 0
	// enquanto existir linhas, será feito consumo dos dados, tratamento de erros ou imprimir o resultado
	for rows.Next() {
		erro := rows.Scan(&totalRegistros)
		registrarErro(erro)
	}

	// tratar erros na query
	erro = rows.Err()
	registrarErro(erro)

	if totalRegistros == 0 {
		fmt.Println("Sem dados")

		// tratar o arquivo .txt para inserir os dados na tabela
		arquivo, erro := os.Open("base_teste.txt")
		registrarErro(erro)
		defer arquivo.Close()

		// ler arquivo em linhas
		linhas := bufio.NewScanner(arquivo)

		// contador para não permitir tratar os registros da primeira linha (cabeçalho do arquivo)
		var contador int = 0
		var dadosLinha string = ""

		// percorrer as linhas para tratar e manipular os dados
		for linhas.Scan() {
			// função para substituir caracteres redundantes de uma string, neste caso o espaço em branco
			// substituir por outro caracter "convencional" para identificar um separador, o ponto e virgula
			dadosLinha = strings.Join(strings.Fields(linhas.Text()), ";")

			fmt.Println(strings.Join(strings.Fields(dadosLinha), ";"))

			if contador > 0 {
				//quebrar registro no separador para determinar os valores para os campos do insert
				registro := strings.Split(dadosLinha, ";")
				codCpfPessoa, flgPrivate, flgIncompleto, datUltimaCompra, vlrTicketMedio,
					vlrTicketUltimaCompra, codCnpjLojaMaisFrequente, codCnpjLojaUltimaCompra :=
					registro[0], registro[1], registro[2], registro[3], registro[4], registro[5],
					registro[6], registro[7]

				// eliminar caracteres especiais do documento
				limparCaracteresCpfCnpj(&codCpfPessoa)

				if datUltimaCompra == "NULL" {
					datUltimaCompra = "1900-01-01"
				}

				//tratar caracteres especiais e conversão do VLR TICKET MEDIO para float
				vlrTicketMedio = strings.ReplaceAll(vlrTicketMedio, ",", ".")
				var vlrTicketMedioInsert float64 = 0
				if vlrTicketMedio != "NULL" {
					convert, erro := strconv.ParseFloat(vlrTicketMedio, BITSIZE)
					registrarErro(erro)

					vlrTicketMedioInsert = convert
				}

				//tratar caracteres especiais e conversão do VLR ULTIMA COMPRA para float
				vlrTicketUltimaCompra = strings.ReplaceAll(vlrTicketUltimaCompra, ",", ".")
				var vlrTicketUltimaCompraInsert float64 = 0
				if vlrTicketUltimaCompra != "NULL" {
					convert, erro := strconv.ParseFloat(vlrTicketUltimaCompra, BITSIZE)
					registrarErro(erro)

					vlrTicketUltimaCompraInsert = convert
				}

				if codCnpjLojaMaisFrequente != "NULL" {
					// eliminar caracteres especiais do documento
					limparCaracteresCpfCnpj(&codCnpjLojaMaisFrequente)
				}

				if codCnpjLojaUltimaCompra != "NULL" {
					// eliminar caracteres especiais do documento
					limparCaracteresCpfCnpj(&codCnpjLojaUltimaCompra)
				}

				// verificar se o tamanho da string do CPF é compatível com o tamanho máximo permitido
				if len(codCpfPessoa) == 11 {
					// comando da query de insert
					sqlInsert := `INSERT INTO PESSOA 
				(
					COD_CPF_PESSOA, 
					FLG_PRIVATE, 
					FLG_INCOMPLETO,
					DAT_ULTIMA_COMPRA,
					VLR_TICKET_MEDIO,
					VLR_TICKET_ULTIMA_COMPRA,
					COD_CNPJ_LOJA_MAIS_FREQUENTE,
					COD_CNPJ_LOJA_ULTIMA_COMPRA,
					DAT_CRIACAO_REGISTRO
				) 
				   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);`

					// executar insert dos dados
					executarInsert, erro := db.Exec(sqlInsert, codCpfPessoa, flgPrivate, flgIncompleto,
						datUltimaCompra, vlrTicketMedioInsert, vlrTicketUltimaCompraInsert,
						codCnpjLojaMaisFrequente, codCnpjLojaUltimaCompra, "NOW()")
					registrarErro(erro)

					// informar se houve confirmação da linha inserida
					rowCount, erro := executarInsert.RowsAffected()
					registrarErro(erro)
					fmt.Println("INSERT EFETIVADO: %d linha afetada.\n", rowCount)
				}
			}

			contador++
		}

		fmt.Println("Concluído com sucesso")
	} else {
		fmt.Println("Dados já inseridos")
	}

}
