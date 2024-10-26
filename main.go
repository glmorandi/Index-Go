package main

import (
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"strconv"
)

type Produto struct {
	ID           [4]byte  // Tamanho fixo para int32 (ID) PK
	ProductID    [4]byte  // Tamanho fixo para int32 (product_id)
	Price        [4]byte  // Tamanho fixo para float32
	Brand        [20]byte // Tamanho fixo para a marca
	CategoryCode [20]byte // Tamanho fixo para o código da categoria
}

type Acesso struct {
	ID          [4]byte  // Tamanho fixo para int32 (ID) PK
	UserSession [20]byte // Tamanho fixo para sessão de usuário
	UserID      [4]byte  // Tamanho fixo para int32 (user_id)
	EventType   [10]byte // Tamanho fixo para o tipo de evento
}

type IndexProduto struct {
	ID     [4]byte // ID do produto (chave)
	Offset [8]byte // Posição do registro no arquivo de produtos
}

type IndexAcesso struct {
	ID     [4]byte // ID do acesso (chave)
	Offset [8]byte // Posição do registro no arquivo de acessos
}

func padString(str string, length int) []byte {
	padded := make([]byte, length)
	copy(padded, str)
	return padded
}

func int32ToBytes(n int32) [4]byte {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], uint32(n))
	return buf
}

func float32ToBytes(f float32) [4]byte {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], math.Float32bits(f))
	return buf
}

func processCSV(filePath string, filenameProd string, filenameAccess string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("erro ao abrir o arquivo CSV: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	_, err = reader.Read()
	if err != nil {
		return fmt.Errorf("erro ao ler o cabeçalho: %w", err)
	}

	fileProd, err := os.Create(filenameProd)
	if err != nil {
		return fmt.Errorf("erro ao criar arquivo de produtos: %w", err)
	}
	defer fileProd.Close()

	fileAcess, err := os.Create(filenameAccess)
	if err != nil {
		return fmt.Errorf("erro ao criar arquivo de acessos: %w", err)
	}
	defer fileAcess.Close()

	produtoIDCounter := int32(1)
	acessoIDCounter := int32(1)

	for {
		record, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return fmt.Errorf("erro ao ler o arquivo CSV: %w", err)
		}

		productID, _ := strconv.ParseInt(record[3], 10, 32) // product_id
		price, _ := strconv.ParseFloat(record[6], 32)       // price
		userID, _ := strconv.ParseInt(record[7], 10, 32)    // user_id
		userSession := record[8]                            // user_session
		eventType := record[1]                              // event_type
		brand := record[5]                                  // brand
		categoryCode := record[4]                           // category_code

		var produto Produto
		produto.ID = int32ToBytes(produtoIDCounter)
		produto.ProductID = int32ToBytes(int32(productID))
		produto.Price = float32ToBytes(float32(price))
		copy(produto.Brand[:], padString(brand, 20))
		copy(produto.CategoryCode[:], padString(categoryCode, 20))

		err = binary.Write(fileProd, binary.LittleEndian, produto)
		if err != nil {
			return fmt.Errorf("erro ao escrever produto no arquivo binário: %w", err)
		}

		var acesso Acesso
		acesso.ID = int32ToBytes(acessoIDCounter)
		copy(acesso.UserSession[:], padString(userSession, 20))
		acesso.UserID = int32ToBytes(int32(userID))
		copy(acesso.EventType[:], padString(eventType, 10))

		err = binary.Write(fileAcess, binary.LittleEndian, acesso)
		if err != nil {
			return fmt.Errorf("erro ao escrever acesso no arquivo: %w", err)
		}

		produtoIDCounter++
		acessoIDCounter++
	}

	return nil
}

func bytesToInt32(b [4]byte) int32 {
	return int32(binary.LittleEndian.Uint32(b[:]))
}

func bytesToFloat32(b [4]byte) float32 {
	return math.Float32frombits(binary.LittleEndian.Uint32(b[:]))
}

func lerArquivoProdutos(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("erro ao abrir o arquivo binário de produtos: %w", err)
	}
	defer file.Close()

	var produto Produto
	count := 0

	for {
		err := binary.Read(file, binary.LittleEndian, &produto)
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return fmt.Errorf("erro ao ler registro de produto: %w", err)
		}

		id := bytesToInt32(produto.ID)
		productID := bytesToInt32(produto.ProductID)
		price := bytesToFloat32(produto.Price)
		brand := string(produto.Brand[:])
		categoryCode := string(produto.CategoryCode[:])

		fmt.Printf("Produto - ID: %d, ProductID: %d, Preço: %.2f, Marca: %s, Categoria: %s\n", id, productID, price, brand, categoryCode)
		count++
	}

	fmt.Printf("Total de produtos lidos: %d\n", count)
	return nil
}

func lerArquivoAcessos(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("erro ao abrir o arquivo binário de acessos: %w", err)
	}
	defer file.Close()

	var acesso Acesso
	count := 0

	for {
		err := binary.Read(file, binary.LittleEndian, &acesso)
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return fmt.Errorf("erro ao ler registro de acesso: %w", err)
		}

		id := bytesToInt32(acesso.ID)
		userSession := string(acesso.UserSession[:])
		userID := bytesToInt32(acesso.UserID)
		eventType := string(acesso.EventType[:])

		fmt.Printf("Acesso - ID: %d, Sessão: %s, UserID: %d, Evento: %s\n", id, userSession, userID, eventType)
		count++
	}

	fmt.Printf("Total de acessos lidos: %d\n", count)
	return nil
}

func inserirProduto(filename string, produto Produto) error {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("erro ao abrir o arquivo de produtos para inserção: %w", err)
	}
	defer file.Close()

	err = binary.Write(file, binary.LittleEndian, produto)
	if err != nil {
		return fmt.Errorf("erro ao escrever produto no arquivo: %w", err)
	}

	return nil
}

func inserirAcesso(filename string, acesso Acesso) error {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("erro ao abrir o arquivo de acessos para inserção: %w", err)
	}
	defer file.Close()

	err = binary.Write(file, binary.LittleEndian, acesso)
	if err != nil {
		return fmt.Errorf("erro ao escrever acesso no arquivo: %w", err)
	}

	return nil
}

func proximoIDProdutos(filename string) (int32, error) {
	file, err := os.Open(filename)
	if err != nil {
		return 0, fmt.Errorf("erro ao abrir o arquivo de produtos: %w", err)
	}
	defer file.Close()

	var produto Produto
	var lastID int32 = 0

	for {
		err := binary.Read(file, binary.LittleEndian, &produto)
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return 0, fmt.Errorf("erro ao ler registro de produto: %w", err)
		}
		lastID = bytesToInt32(produto.ID)
	}

	return lastID + 1, nil
}

func proximoIDAcessos(filename string) (int32, error) {
	file, err := os.Open(filename)
	if err != nil {
		return 0, fmt.Errorf("erro ao abrir o arquivo de acessos: %w", err)
	}
	defer file.Close()

	var acesso Acesso
	var lastID int32 = 0

	for {
		err := binary.Read(file, binary.LittleEndian, &acesso)
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return 0, fmt.Errorf("erro ao ler registro de acesso: %w", err)
		}
		lastID = bytesToInt32(acesso.ID)
	}

	return lastID + 1, nil
}

func bytesToArray10(b []byte) [10]byte {
	var arr [10]byte
	copy(arr[:], b)
	return arr
}

func bytesToArray20(b []byte) [20]byte {
	var arr [20]byte
	copy(arr[:], b)
	return arr
}

func pesquisarProduto(filename string, id int32) (Produto, error) {
	file, err := os.Open(filename)
	if err != nil {
		return Produto{}, fmt.Errorf("erro ao abrir o arquivo binário de produtos: %w", err)
	}
	defer file.Close()

	var produto Produto
	start := 0
	fileInfo, _ := file.Stat()
	size := int(fileInfo.Size() / int64(binary.Size(produto)))

	for start <= size {
		mid := (start + size) / 2
		_, err := file.Seek(int64(mid*binary.Size(produto)), 0)
		if err != nil {
			return Produto{}, fmt.Errorf("erro ao buscar no arquivo: %w", err)
		}

		err = binary.Read(file, binary.LittleEndian, &produto)
		if err != nil {
			return Produto{}, fmt.Errorf("erro ao ler registro de produto: %w", err)
		}

		midID := bytesToInt32(produto.ID)

		if midID == id {
			return produto, nil
		} else if midID < id {
			start = mid + 1
		} else {
			size = mid - 1
		}
	}

	return Produto{}, fmt.Errorf("produto com ID %d não encontrado", id)
}

func pesquisarAcesso(filename string, id int32) (Acesso, error) {
	file, err := os.Open(filename)
	if err != nil {
		return Acesso{}, fmt.Errorf("erro ao abrir o arquivo binário de acessos: %w", err)
	}
	defer file.Close()

	var acesso Acesso
	start := 0
	fileInfo, _ := file.Stat()
	size := int(fileInfo.Size() / int64(binary.Size(acesso)))

	for start <= size {
		mid := (start + size) / 2
		_, err := file.Seek(int64(mid*binary.Size(acesso)), 0)
		if err != nil {
			return Acesso{}, fmt.Errorf("erro ao buscar no arquivo: %w", err)
		}

		err = binary.Read(file, binary.LittleEndian, &acesso)
		if err != nil {
			return Acesso{}, fmt.Errorf("erro ao ler registro de acesso: %w", err)
		}

		midID := bytesToInt32(acesso.ID)

		if midID == id {
			return acesso, nil
		} else if midID < id {
			start = mid + 1
		} else {
			size = mid - 1
		}
	}

	return Acesso{}, fmt.Errorf("acesso com ID %d não encontrado", id)
}

func encontrarProdutoMaisCaro(filename string) (Produto, error) {
	file, err := os.Open(filename)
	if err != nil {
		return Produto{}, fmt.Errorf("erro ao abrir o arquivo de produtos: %w", err)
	}
	defer file.Close()

	var produto Produto
	var produtoMaisCaro Produto
	maiorPreco := float32(0)

	count := 0
	fileInfo, _ := file.Stat()
	size := int(fileInfo.Size() / int64(binary.Size(produto)))

	for i := 0; i < size; i++ {
		_, err := file.Seek(int64(i*binary.Size(produto)), 0)
		if err != nil {
			return Produto{}, fmt.Errorf("erro ao mover o cursor para o offset: %w", err)
		}

		err = binary.Read(file, binary.LittleEndian, &produto)
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return Produto{}, fmt.Errorf("erro ao ler registro de produto: %w", err)
		}

		preco := bytesToFloat32(produto.Price)
		if preco > maiorPreco {
			maiorPreco = preco
			produtoMaisCaro = produto
		}

		count++
	}

	if count == 0 {
		return Produto{}, fmt.Errorf("nenhum produto encontrado")
	}

	return produtoMaisCaro, nil
}

func criarIndiceProdutos(filenameProd string, indexProd string) error {
	file, err := os.Open(filenameProd)
	if err != nil {
		return fmt.Errorf("erro ao abrir o arquivo de produtos: %w", err)
	}
	defer file.Close()

	indexFile, err := os.Create(indexProd)
	if err != nil {
		return fmt.Errorf("erro ao criar arquivo de índice de produtos: %w", err)
	}
	defer indexFile.Close()

	var produto Produto
	offset := int64(0)

	for {
		err := binary.Read(file, binary.LittleEndian, &produto)
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return fmt.Errorf("erro ao ler registro de produto: %w", err)
		}

		var index IndexProduto
		index.ID = produto.ID
		binary.LittleEndian.PutUint64(index.Offset[:], uint64(offset))

		err = binary.Write(indexFile, binary.LittleEndian, index)
		if err != nil {
			return fmt.Errorf("erro ao escrever índice de produto: %w", err)
		}

		offset += int64(binary.Size(produto))
	}

	return nil
}

func userSessionMaisFrequente(filename string) (string, int, error) {
	file, err := os.Open(filename)
	if err != nil {
		return "", 0, fmt.Errorf("erro ao abrir o arquivo de acessos: %w", err)
	}
	defer file.Close()

	var acesso Acesso
	sessaoCount := make(map[string]int)
	for {
		err := binary.Read(file, binary.LittleEndian, &acesso)
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return "", 0, fmt.Errorf("erro ao ler registro de acesso: %w", err)
		}

		sessao := string(acesso.UserSession[:])
		sessaoCount[sessao]++
	}
	var sessaoMaisFrequente string
	var maxCount int

	for sessao, count := range sessaoCount {
		if count > maxCount {
			maxCount = count
			sessaoMaisFrequente = sessao
		}
	}

	return sessaoMaisFrequente, maxCount, nil
}

func criarIndiceAcessos(filenameAccess string, indexAccess string) error {
	file, err := os.Open(filenameAccess)
	if err != nil {
		return fmt.Errorf("erro ao abrir o arquivo de acessos: %w", err)
	}
	defer file.Close()

	indexFile, err := os.Create(indexAccess)
	if err != nil {
		return fmt.Errorf("erro ao criar arquivo de índice de acessos: %w", err)
	}
	defer indexFile.Close()

	var acesso Acesso
	offset := int64(0)

	for {
		err := binary.Read(file, binary.LittleEndian, &acesso)
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return fmt.Errorf("erro ao ler registro de acesso: %w", err)
		}

		var index IndexAcesso
		index.ID = acesso.ID
		binary.LittleEndian.PutUint64(index.Offset[:], uint64(offset))

		err = binary.Write(indexFile, binary.LittleEndian, index)
		if err != nil {
			return fmt.Errorf("erro ao escrever índice de acesso: %w", err)
		}

		offset += int64(binary.Size(acesso))
	}

	return nil
}

func consultarProdutoComIndice(indexProd string, filenameProd string, id int32) (Produto, error) {
	indexFile, err := os.Open(indexProd)
	if err != nil {
		return Produto{}, fmt.Errorf("erro ao abrir o arquivo de índice de produtos: %w", err)
	}
	defer indexFile.Close()

	var index IndexProduto
	start := 0
	indexFileInfo, _ := indexFile.Stat()
	indexSize := int(indexFileInfo.Size() / int64(binary.Size(index)))

	for start <= indexSize {
		mid := (start + indexSize) / 2
		_, err := indexFile.Seek(int64(mid*binary.Size(index)), 0)
		if err != nil {
			return Produto{}, fmt.Errorf("erro ao buscar no arquivo de índice: %w", err)
		}

		err = binary.Read(indexFile, binary.LittleEndian, &index)
		if err != nil {
			return Produto{}, fmt.Errorf("erro ao ler registro de índice de produto: %w", err)
		}

		midID := bytesToInt32(index.ID)

		if midID == id {
			offset := binary.LittleEndian.Uint64(index.Offset[:])
			produto, err := buscarProdutoPorOffset(filenameProd, int64(offset))
			return produto, err
		} else if midID < id {
			start = mid + 1
		} else {
			indexSize = mid - 1
		}
	}

	return Produto{}, fmt.Errorf("produto com ID %d não encontrado", id)
}

func consultarAcessoComIndice(indexAccess string, filenameAccess string, id int32) (Acesso, error) {
	indexFile, err := os.Open(indexAccess)
	if err != nil {
		return Acesso{}, fmt.Errorf("erro ao abrir o arquivo de índice de acessos: %w", err)
	}
	defer indexFile.Close()

	var index IndexAcesso
	start := 0
	indexFileInfo, _ := indexFile.Stat()
	indexSize := int(indexFileInfo.Size() / int64(binary.Size(index)))

	for start <= indexSize {
		mid := (start + indexSize) / 2
		_, err := indexFile.Seek(int64(mid*binary.Size(index)), 0)
		if err != nil {
			return Acesso{}, fmt.Errorf("erro ao buscar no arquivo de índice: %w", err)
		}

		err = binary.Read(indexFile, binary.LittleEndian, &index)
		if err != nil {
			return Acesso{}, fmt.Errorf("erro ao ler registro de índice de acesso: %w", err)
		}

		midID := bytesToInt32(index.ID)

		if midID == id {
			offset := binary.LittleEndian.Uint64(index.Offset[:])
			acesso, err := buscarAcessoPorOffset(filenameAccess, int64(offset))
			return acesso, err
		} else if midID < id {
			start = mid + 1
		} else {
			indexSize = mid - 1
		}
	}

	return Acesso{}, fmt.Errorf("acesso com ID %d não encontrado", id)
}

func buscarProdutoPorOffset(filename string, offset int64) (Produto, error) {
	file, err := os.Open(filename)
	if err != nil {
		return Produto{}, fmt.Errorf("erro ao abrir o arquivo de produtos: %w", err)
	}
	defer file.Close()

	var produto Produto
	_, err = file.Seek(offset, 0)
	if err != nil {
		return Produto{}, fmt.Errorf("erro ao mover o cursor para o offset: %w", err)
	}

	err = binary.Read(file, binary.LittleEndian, &produto)
	if err != nil {
		return Produto{}, fmt.Errorf("erro ao ler registro de produto: %w", err)
	}

	return produto, nil
}

func buscarAcessoPorOffset(filename string, offset int64) (Acesso, error) {
	file, err := os.Open(filename)
	if err != nil {
		return Acesso{}, fmt.Errorf("erro ao abrir o arquivo de acessos: %w", err)
	}
	defer file.Close()

	var acesso Acesso
	_, err = file.Seek(offset, 0)
	if err != nil {
		return Acesso{}, fmt.Errorf("erro ao mover o cursor para o offset: %w", err)
	}

	err = binary.Read(file, binary.LittleEndian, &acesso)
	if err != nil {
		return Acesso{}, fmt.Errorf("erro ao ler registro de acesso: %w", err)
	}

	return acesso, nil
}

func inserirProdutoECriarIndice(filenameProd string, filenameIndex string, produto Produto) error {
	err := inserirProduto(filenameProd, produto)
	if err != nil {
		return err
	}

	return criarIndiceProdutos(filenameProd, filenameIndex)
}

func inserirAcessoECriarIndice(filenameAccess string, filenameIndex string, acesso Acesso) error {
	err := inserirAcesso(filenameAccess, acesso)
	if err != nil {
		return err
	}

	return criarIndiceAcessos(filenameAccess, filenameIndex)
}

func removerProduto(filenameProd string, id int32) error {
	tempFile, err := os.Create("temp_produtos.bin")
	if err != nil {
		return fmt.Errorf("erro ao criar arquivo temporário para produtos: %w", err)
	}
	defer tempFile.Close()

	file, err := os.Open(filenameProd)
	if err != nil {
		return fmt.Errorf("erro ao abrir o arquivo de produtos: %w", err)
	}
	defer file.Close()

	var produto Produto
	found := false

	for {
		err := binary.Read(file, binary.LittleEndian, &produto)
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return fmt.Errorf("erro ao ler registro de produto: %w", err)
		}

		if bytesToInt32(produto.ID) == id {
			found = true
			continue
		}

		err = binary.Write(tempFile, binary.LittleEndian, produto)
		if err != nil {
			return fmt.Errorf("erro ao escrever produto no arquivo temporário: %w", err)
		}
	}

	if !found {
		return fmt.Errorf("produto com ID %d não encontrado", id)
	}

	err = os.Rename("temp_produtos.bin", filenameProd)
	if err != nil {
		return fmt.Errorf("erro ao renomear arquivo temporário: %w", err)
	}

	return criarIndiceProdutos(filenameProd, "indice_produtos.bin")
}

func removerAcesso(filenameAccess string, id int32) error {
	tempFile, err := os.Create("temp_acessos.bin")
	if err != nil {
		return fmt.Errorf("erro ao criar arquivo temporário para acessos: %w", err)
	}
	defer tempFile.Close()

	file, err := os.Open(filenameAccess)
	if err != nil {
		return fmt.Errorf("erro ao abrir o arquivo de acessos: %w", err)
	}
	defer file.Close()

	var acesso Acesso
	found := false

	for {
		err := binary.Read(file, binary.LittleEndian, &acesso)
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return fmt.Errorf("erro ao ler registro de acesso: %w", err)
		}

		if bytesToInt32(acesso.ID) == id {
			found = true
			continue
		}

		err = binary.Write(tempFile, binary.LittleEndian, acesso)
		if err != nil {
			return fmt.Errorf("erro ao escrever acesso no arquivo temporário: %w", err)
		}
	}

	if !found {
		return fmt.Errorf("acesso com ID %d não encontrado", id)
	}

	err = os.Rename("temp_acessos.bin", filenameAccess)
	if err != nil {
		return fmt.Errorf("erro ao renomear arquivo temporário: %w", err)
	}

	return criarIndiceAcessos(filenameAccess, "indice_acessos.bin")
}

func main() {
	filePath := "t.csv"
	filenameProd := "produtos.bin"
	filenameAccess := "acessos.bin"

	if err := processCSV(filePath, filenameProd, filenameAccess); err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("Arquivos binários criados com sucesso!")

	fmt.Println("Lendo arquivo de produtos:")
	err := lerArquivoProdutos(filenameProd)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println("\nLendo arquivo de acessos:")
	err = lerArquivoAcessos(filenameAccess)
	if err != nil {
		fmt.Println(err)
	}

	novoProduto := Produto{
		ID:           int32ToBytes(0),
		ProductID:    int32ToBytes(12345),
		Price:        float32ToBytes(99.99),
		Brand:        bytesToArray20(padString("Nova Marca", 20)),
		CategoryCode: bytesToArray20(padString("Nova Categoria", 20)),
	}
	proximoID, err := proximoIDProdutos("produtos.bin")
	if err != nil {
		fmt.Println(err)
		return
	}
	novoProduto.ID = int32ToBytes(proximoID)

	if err := inserirProduto("produtos.bin", novoProduto); err != nil {
		fmt.Println(err)
	}

	novoAcesso := Acesso{
		ID:          int32ToBytes(0),
		UserSession: bytesToArray20(padString("sessao123", 20)),
		UserID:      int32ToBytes(67890),
		EventType:   bytesToArray10(padString("view", 10)),
	}
	proximoIDAccess, err := proximoIDAcessos("acessos.bin")
	if err != nil {
		fmt.Println(err)
		return
	}
	novoAcesso.ID = int32ToBytes(proximoIDAccess)

	if err := inserirAcesso("acessos.bin", novoAcesso); err != nil {
		fmt.Println(err)
	}

	fmt.Println("Lendo arquivo de produtos:")
	err = lerArquivoProdutos(filenameProd)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println("\nLendo arquivo de acessos:")
	err = lerArquivoAcessos(filenameAccess)
	if err != nil {
		fmt.Println(err)
	}

	produtoIDParaPesquisar := int32(999)
	produtoEncontrado, err := pesquisarProduto(filenameProd, produtoIDParaPesquisar)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Printf("Produto encontrado: ID: %d, ProductID: %d, Preço: %.2f, Marca: %s, Categoria: %s\n",
			bytesToInt32(produtoEncontrado.ID),
			bytesToInt32(produtoEncontrado.ProductID),
			bytesToFloat32(produtoEncontrado.Price),
			string(produtoEncontrado.Brand[:]),
			string(produtoEncontrado.CategoryCode[:]),
		)
	}

	acessoIDParaPesquisar := int32(999)
	acessoEncontrado, err := pesquisarAcesso(filenameAccess, acessoIDParaPesquisar)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Printf("Acesso encontrado: ID: %d, Sessão: %s, UserID: %d, Evento: %s\n",
			bytesToInt32(acessoEncontrado.ID),
			string(acessoEncontrado.UserSession[:]),
			bytesToInt32(acessoEncontrado.UserID),
			string(acessoEncontrado.EventType[:]),
		)
	}

	produtoMaisCaro, err := encontrarProdutoMaisCaro(filenameProd)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Printf("Produto mais caro: ID: %d, ProductID: %d, Preço: %.2f, Marca: %s, Categoria: %s\n",
			bytesToInt32(produtoMaisCaro.ID),
			bytesToInt32(produtoMaisCaro.ProductID),
			bytesToFloat32(produtoMaisCaro.Price),
			string(produtoMaisCaro.Brand[:]),
			string(produtoMaisCaro.CategoryCode[:]),
		)
	}

	sessao, count, err := userSessionMaisFrequente(filenameAccess)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Printf("A UserSession mais frequente é: %s, com %d ocorrências.\n", sessao, count)
	}

	indexProd := "indice_produtos.dat"
	indexAccess := "indice_acessos.dat"

	err = criarIndiceProdutos(filenameProd, indexProd)
	if err != nil {
		fmt.Println("Erro ao criar índice de produtos:", err)
		return
	}

	err = criarIndiceAcessos(filenameAccess, indexAccess)
	if err != nil {
		fmt.Println("Erro ao criar índice de acessos:", err)
		return
	}

	produtoIDParaConsultar := int32(998)
	produtoEncontrado, err = consultarProdutoComIndice(indexProd, filenameProd, produtoIDParaConsultar)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Printf("Produto encontrado: ID: %d, ProductID: %d, Preço: %.2f, Marca: %s, Categoria: %s\n",
			bytesToInt32(produtoEncontrado.ID),
			bytesToInt32(produtoEncontrado.ProductID),
			bytesToFloat32(produtoEncontrado.Price),
			string(produtoEncontrado.Brand[:]),
			string(produtoEncontrado.CategoryCode[:]),
		)
	}

	acessoIDParaConsultar := int32(998)
	acessoEncontrado, err = consultarAcessoComIndice(indexAccess, filenameAccess, acessoIDParaConsultar)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Printf("Acesso encontrado: ID: %d, Sessão: %s, UserID: %d, Evento: %s\n",
			bytesToInt32(acessoEncontrado.ID),
			string(acessoEncontrado.UserSession[:]),
			bytesToInt32(acessoEncontrado.UserID),
			string(acessoEncontrado.EventType[:]),
		)
	}

	novoProduto = Produto{
		ID:           int32ToBytes(0),
		ProductID:    int32ToBytes(12345),
		Price:        float32ToBytes(99.99),
		Brand:        bytesToArray20(padString("Nova Marca", 20)),
		CategoryCode: bytesToArray20(padString("Nova Categoria", 20)),
	}
	proximoID, err = proximoIDProdutos(filenameProd)
	if err != nil {
		fmt.Println(err)
		return
	}
	novoProduto.ID = int32ToBytes(proximoID)

	if err := inserirProdutoECriarIndice("produtos.bin", indexProd, novoProduto); err != nil {
		fmt.Println(err)
	}

	novoAcesso = Acesso{
		ID:          int32ToBytes(0),
		UserSession: bytesToArray20(padString("sessao123", 20)),
		UserID:      int32ToBytes(67890),
		EventType:   bytesToArray10(padString("view", 10)),
	}
	proximoIDAccess, err = proximoIDAcessos(filenameAccess)
	if err != nil {
		fmt.Println(err)
		return
	}
	novoAcesso.ID = int32ToBytes(proximoIDAccess)

	if err := inserirAcessoECriarIndice("acessos.bin", indexAccess, novoAcesso); err != nil {
		fmt.Println(err)
	}

	produtoIDParaConsultar = int32(1001)
	produtoEncontrado, err = consultarProdutoComIndice(indexProd, filenameProd, produtoIDParaConsultar)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Printf("Produto encontrado: ID: %d, ProductID: %d, Preço: %.2f, Marca: %s, Categoria: %s\n",
			bytesToInt32(produtoEncontrado.ID),
			bytesToInt32(produtoEncontrado.ProductID),
			bytesToFloat32(produtoEncontrado.Price),
			string(produtoEncontrado.Brand[:]),
			string(produtoEncontrado.CategoryCode[:]),
		)
	}

	acessoIDParaConsultar = int32(1001)
	acessoEncontrado, err = consultarAcessoComIndice(indexAccess, filenameAccess, acessoIDParaConsultar)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Printf("Acesso encontrado: ID: %d, Sessão: %s, UserID: %d, Evento: %s\n",
			bytesToInt32(acessoEncontrado.ID),
			string(acessoEncontrado.UserSession[:]),
			bytesToInt32(acessoEncontrado.UserID),
			string(acessoEncontrado.EventType[:]),
		)
	}

	produtoIdParaRemover := int32(1001)
	err = removerProduto(filenameProd, produtoIdParaRemover)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Printf("Removido o item %d do arquivo %s\n", produtoIdParaRemover, filenameProd)
	}

	acessoIdParaRemover := int32(1001)
	err = removerAcesso(filenameAccess, acessoIdParaRemover)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Printf("Removido o item %d do arquivo %s\n", acessoIdParaRemover, filenameAccess)
	}

	produtoIDParaConsultar = int32(1001)
	produtoEncontrado, err = consultarProdutoComIndice(indexProd, filenameProd, produtoIDParaConsultar)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Printf("Produto encontrado: ID: %d, ProductID: %d, Preço: %.2f, Marca: %s, Categoria: %s\n",
			bytesToInt32(produtoEncontrado.ID),
			bytesToInt32(produtoEncontrado.ProductID),
			bytesToFloat32(produtoEncontrado.Price),
			string(produtoEncontrado.Brand[:]),
			string(produtoEncontrado.CategoryCode[:]),
		)
	}

	acessoIDParaConsultar = int32(1001)
	acessoEncontrado, err = consultarAcessoComIndice(indexAccess, filenameAccess, acessoIDParaConsultar)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Printf("Acesso encontrado: ID: %d, Sessão: %s, UserID: %d, Evento: %s\n",
			bytesToInt32(acessoEncontrado.ID),
			string(acessoEncontrado.UserSession[:]),
			bytesToInt32(acessoEncontrado.UserID),
			string(acessoEncontrado.EventType[:]),
		)
	}

}
