# goar 功能总结

## 📖 概述

**goar** 是一个功能完整的 Arweave Go 语言 SDK，提供了与 Arweave 网络交互的所有核心功能，包括交易创建、数据上传、钱包管理、Bundle 打包等。

**项目地址**: https://github.com/permadao/goar

**依赖 Go 版本**: 1.22.4+

---

## 🏗️ 核心架构

### 主要组件

```
goar
├── Client          # Arweave 网络客户端（只读操作）
├── Wallet          # 钱包（签名 + 发送交易）
├── Signer          # 签名器（RSA 签名）
├── ECDSASigner     # ECDSA 签名器（secp256k1）
├── Bundler         # Bundle 打包器
└── Utils           # 工具函数包
```

---

## 🔧 功能模块详解

### 1. Client（网络客户端）

**功能定位**: 与 Arweave 网络交互的只读客户端

#### 初始化
```go
// 基础用法
arClient := goar.NewClient("https://arweave.net")

// 使用 HTTP 代理
proxyUrl := "http://127.0.0.1:8001"
arClient := goar.NewClient("https://arweave.net", proxyUrl)

// 设置超时
arClient.SetTimeout(30 * time.Second)
```

#### 核心功能

##### 网络信息
```go
// 获取网络信息
info, err := arClient.GetInfo()
// 返回：NetworkInfo {
//   Version: "1.13.0",
//   Height: 1234567,
//   Current: "block_hash",
//   ...
// }

// 获取节点列表
peers, err := arClient.GetPeers()
```

##### 交易查询
```go
// 根据 ID 获取交易
tx, err := arClient.GetTransactionByID(txID)

// 获取交易状态
status, err := arClient.GetTransactionStatus(txID)
// 返回：TxStatus {
//   BlockHeight: 1234567,
//   BlockIndepHash: "xxx",
//   NumberOfConfirmations: 10,
// }

// 获取交易字段
dataSize, err := arClient.GetTransactionField(txID, "data_size")

// 获取交易标签
tags, err := arClient.GetTransactionTags(txID)

// 获取交易数据
data, err := arClient.GetTransactionData(txID)
```

##### 费用计算
```go
// 获取交易价格（字节数，目标地址）
reward, err := arClient.GetTransactionPrice(1024, &target)

// 获取交易锚点（用于防止重放攻击）
anchor, err := arClient.GetTransactionAnchor()
```

##### 区块查询
```go
// 根据 ID 获取区块
block, err := arClient.GetBlockByID(blockID)

// 根据高度获取区块
block, err := arClient.GetBlockByHeight(1234567)
```

##### 钱包相关
```go
// 获取钱包余额（winston 单位）
balance, err := arClient.GetWalletBalance(address)

// 获取最后一次交易 ID
lastTxID, err := arClient.GetLastTransactionID(address)
```

##### GraphQL 查询
```go
// 执行 GraphQL 查询
result, err := arClient.GraphQL(query)
```

##### 高级功能
```go
// 从 peers 获取交易数据
data, err := arClient.GetTxDataFromPeers(txID)

// 获取未确认交易
tx, err := arClient.GetUnconfirmedTx(txID)

// 获取待处理交易列表
pendingTxIds, err := arClient.GetPendingTxIds()

// 获取区块哈希列表
hashList, err := arClient.GetBlockHashList(start, end)

// 并发下载 chunk 数据
err := arClient.ConcurrentDownloadChunkData(txID, concurrentNum)
```

---

### 2. Wallet（钱包）

**功能定位**: 完整的钱包功能，支持发送 AR、数据和 PST

#### 初始化
```go
// 从 keyfile.json 加载
wallet, err := goar.NewWalletFromPath("./keyfile.json", "https://arweave.net")

// 使用代理
wallet, err := goar.NewWalletFromPath("./keyfile.json", "https://arweave.net", "http://127.0.0.1:8001")

// 从字节加载
wallet, err := goar.NewWallet(keyBytes, "https://arweave.net")

// 使用自定义签名器
signer, _ := goar.NewSignerFromPath("./keyfile.json")
wallet := goar.NewWalletWithSigner(signer, "https://arweave.net")
```

#### 发送 AR/Winston

##### 基础发送
```go
import "math/big"

// 发送 AR
tx, err := wallet.SendAR(
    big.NewFloat(1.0),      // AR 数量
    targetAddress,          // 目标地址
    []schema.Tag{...},      // 标签
)

// 发送 Winston
tx, err := wallet.SendWinston(
    big.NewInt(1000000),    // Winston 数量
    targetAddress,
    []schema.Tag{...},
)
```

##### 加速发送
```go
// 加速发送 AR（speedFactor = 50 表示奖励增加 50%）
tx, err := wallet.SendARSpeedUp(
    big.NewFloat(1.0),
    targetAddress,
    []schema.Tag{...},
    50,  // speedFactor
)

// 加速发送 Winston
tx, err := wallet.SendWinstonSpeedUp(
    big.NewInt(1000000),
    targetAddress,
    []schema.Tag{...},
    50,
)
```

#### 发送数据

##### 基础发送
```go
// 发送字节数据
tx, err := wallet.SendData(
    []byte("Hello Arweave"),
    []schema.Tag{
        {Name: "Content-Type", Value: "text/plain"},
        {Name: "App-Name", Value: "MyApp"},
    },
)

// 发送文件流
file, _ := os.Open("large_file.bin")
tx, err := wallet.SendDataStream(
    file,
    []schema.Tag{...},
)
```

##### 加速发送
```go
// 加速发送数据
tx, err := wallet.SendDataSpeedUp(
    []byte("data"),
    []schema.Tag{...},
    50,  // speedFactor: 奖励增加 50%
)

// 加速发送文件流
file, _ := os.Open("large_file.bin")
tx, err := wallet.SendDataStreamSpeedUp(
    file,
    []schema.Tag{...},
    50,
)
```

##### 并发发送（大文件优化）
```go
ctx := context.Background()

// 并发发送字节数据
tx, err := wallet.SendDataConcurrentSpeedUp(
    ctx,
    10,              // 并发数
    []byte("data"),  // 数据
    []schema.Tag{...},
    50,              // speedFactor
)

// 并发发送文件
file, _ := os.Open("large_file.bin")
tx, err := wallet.SendDataConcurrentSpeedUp(
    ctx,
    10,        // 并发数
    file,      // 文件
    []schema.Tag{...},
    50,
)
```

#### Bundle 相关

##### 发送 Bundle 交易
```go
ctx := context.Background()

// 发送 Bundle 二进制数据
tx, err := wallet.SendBundleTx(
    ctx,
    10,              // 并发数
    bundleBinary,    // Bundle 二进制数据
    []schema.Tag{...},
)

// 加速发送 Bundle
tx, err := wallet.SendBundleTxSpeedUp(
    ctx,
    10,
    bundleBinary,
    []schema.Tag{...},
    50,  // speedFactor
)

// 发送 Bundle 流
file, _ := os.Open("bundle.bin")
tx, err := wallet.SendBundleTxStream(
    ctx,
    10,
    file,
    []schema.Tag{...},
)
```

#### 发送 PST (Arweave 代币标准)

```go
tx, err := wallet.SendPst(
    pstContract,    // PST 合约地址
    big.NewInt(100), // 数量
    targetAddress,
    []schema.Tag{...},
)
```

#### 获取钱包信息

```go
// 获取 Owner（公钥的 Base64 编码）
owner := wallet.Owner()

// 获取签名器
signer := wallet.Signer
```

---

### 3. Signer（签名器）

**功能定位**: 交易和数据签名

#### RSA 签名器（Arweave 原生）

##### 初始化
```go
// 从文件加载
signer, err := goar.NewSignerFromPath("./keyfile.json")

// 从字节加载
signer, err := goar.NewSigner(keyBytes)

// 从私钥创建
signer := goar.NewSignerByPrivateKey(rsaPrivateKey)
```

##### 签名功能
```go
// 签名交易
err := signer.SignTx(tx)

// 签名消息
sigBytes, err := signer.SignMsg(msgBytes)

// 获取 Owner
owner := signer.Owner()  // Base64 编码的公钥

// 获取地址
address := signer.Address
```

#### ECDSA 签名器（以太坊兼容）

##### 初始化
```go
// 从十六进制私钥创建
privateKey := "ccb43edab9f7fd5c24388c43579b9d50ad3c8d94cf5fce9bb0bcb8e6ddb57bce"
ecSigner, err := goar.NewEcSigner(privateKey)
```

##### 签名功能
```go
// 获取地址
address := ecSigner.Address()

// 获取 Owner（压缩公钥）
owner := ecSigner.Owner()

// 签名交易
err := ecSigner.SignTx(tx)

// 验证交易签名
valid := goar.VerifyEcdsaTxSig(tx)

// 获取交易 Owner
owner := goar.GetEcdsaTxOwner(tx)

// Owner 转地址
address := goar.OwnerToAddress(owner)
```

---

### 4. Bundler（Bundle 打包器）

**功能定位**: 创建和签名 ANS-104 Bundle

#### 初始化
```go
// 使用 RSA 签名器
rsaSigner, _ := goar.NewSignerFromPath("./keyfile.json")
bundler, _ := goar.NewBundler(rsaSigner)

// 使用以太坊签名器
ethSigner, _ := goether.NewSignerFromPath("./eth-keyfile.json")
bundler, _ := goar.NewBundler(ethSigner)
```

#### 创建和签名 Item

##### 基础 Item
```go
item, err := bundler.CreateAndSignItem(
    []byte("data"),           // 数据
    targetAddress,            // 目标地址（可选）
    anchor,                   // 锚点（可选）
    []schema.Tag{...},        // 标签
)
// 返回：schema.BundleItem
```

##### 嵌套 Item（包含多个 Item 的 Bundle）
```go
// 先创建子 Item
item1, _ := bundler.CreateAndSignItem(data1, "", "", tags1)
item2, _ := bundler.CreateAndSignItem(data2, "", "", tags2)

// 创建嵌套 Item
nestedItem, err := bundler.CreateAndSignNestedItem(
    targetAddress,
    anchor,
    []schema.Tag{
        {"App-Name", "MyApp"},
    },
    item1, item2,  // 子 Item 列表
)
```

#### 手动签名
```go
item := schema.BundleItem{
    SignatureType: schema.ArweaveSignType,
    Target:        targetAddress,
    Anchor:        anchor,
    Tags:          tags,
    Data:          base64Data,
}

err := bundler.Sign(&item)
```

---

### 5. TransactionUploader（交易上传器）

**功能定位**: 管理大文件的分块上传

#### 创建上传器
```go
// 从交易创建
uploader, err := goar.CreateUploader(
    arClient,
    tx,        // 已签名的交易
    dataBytes, // 数据
)

// 从交易 ID 恢复
uploader, err := goar.CreateUploader(
    arClient,
    txID,      // 交易 ID
    dataBytes, // 数据
)
```

#### 上传功能

##### 单次上传
```go
err := uploader.Once()
```

##### 并发上传
```go
ctx := context.Background()
err := uploader.ConcurrentOnce(ctx, 10)  // 10 个并发
```

##### 状态查询
```go
// 是否完成
isComplete := uploader.IsComplete()

// 总 chunk 数
total := uploader.TotalChunks()

// 已上传 chunk 数
uploaded := uploader.UploadedChunks()

// 完成百分比
pct := uploader.PctComplete()  // 0-100
```

#### 序列化上传器（用于断点续传）
```go
// 序列化为 SerializedUploader
serialized := uploader.ToSerialized()

// 从序列化数据恢复
uploader, err := (&TransactionUploader{Client: api}).FromSerialized(
    serialized,
    dataBytes,
)
```

---

## 📊 数据结构

### Transaction（交易）
```go
type Transaction struct {
    Format     int           `json:"format"`      // 交易格式版本
    ID         string        `json:"id"`          // 交易 ID
    LastTx     string        `json:"last_tx"`     // 上一次交易 ID
    Owner      string        `json:"owner"`       // 所有者（Base64 公钥）
    Tags       []Tag         `json:"tags"`        // 标签列表
    Target     string        `json:"target"`      // 目标地址
    Quantity   string        `json:"quantity"`    // 转账数量（winston）
    Data       string        `json:"data"`        // Base64 编码的数据
    DataReader *os.File      `json:"-"`           // 大数据用文件流
    DataSize   string        `json:"data_size"`   // 数据大小
    DataRoot   string        `json:"data_root"`   // Merkle 树根
    Reward     string        `json:"reward"`      // 奖励（winston）
    Signature  string        `json:"signature"`   // 签名
    Chunks     *Chunks       `json:"-"`           // 分块信息
}
```

### BundleItem（Bundle 项）
```go
type BundleItem struct {
    SignatureType int      `json:"signatureType"`  // 签名类型
    Signature     string   `json:"signature"`      // 签名（Base64）
    Owner         string   `json:"owner"`          // 所有者（Base64 公钥）
    Target        string   `json:"target"`         // 目标地址（可选）
    Anchor        string   `json:"anchor"`         // 锚点（可选）
    Tags          []Tag    `json:"tags"`           // 标签
    Data          string   `json:"data"`           // Base64 数据
    Id            string   `json:"id"`             // Item ID
    TagsBy        string   `json:"tagsBy"`         // 标签的 Base64
    
    Binary        []byte   `json:"-"`              // 二进制数据
    DataReader    *os.File `json:"-"`              // 文件流
}
```

### Tag（标签）
```go
type Tag struct {
    Name  string `json:"name"`   // Base64 编码
    Value string `json:"value"`  // Base64 编码
}
```

### 签名类型常量
```go
const (
    ArweaveSignType  = 1  // RSA 签名
    ED25519SignType  = 2  // Ed25519 签名
    EthereumSignType = 3  // ECDSA secp256k1
    SolanaSignType   = 4  // Solana 签名
)
```

---

## 🛠️ 工具函数 (utils)

### 编码转换
```go
// Base64 编码/解码
encoded := utils.Base64Encode(bytes)
decoded, err := utils.Base64Decode(encodedStr)

// AR 转 Winston
winston := utils.ARToWinston(big.NewFloat(1.0))

// Winston 转 AR
ar := utils.WinstonToAR(big.NewInt(1000000000))
```

### 标签处理
```go
// 编码标签
encodedTags := utils.TagsEncode(tags)

// 解码标签
tags, err := utils.TagsDecode(encodedTags)
```

### Merkle 树
```go
// 构建 Merkle 树
tree, err := utils.BuildMerkleTree(dataChunks)

// 获取 Merkle 根
root := tree.Root()

// 生成证明
proof := tree.GetProof(chunkIndex)
```

### Bundle 工具
```go
// 创建 Bundle
bundle, err := utils.NewBundle(item1, item2, ...)

// 创建 Bundle 流
bundle, err := utils.NewBundleStream(item1, item2, ...)

// 解码 Bundle
bundle, err := utils.DecodeBundle(bundleBinary)

// 生成 Item 二进制
binary, err := utils.GenerateItemBinary(item)
```

### 签名工具
```go
// 签名交易
err := utils.SignTransaction(tx, rsaPrivateKey)

// 签名消息
sig, err := utils.Sign(msg, rsaPrivateKey)

// Bundle Item 签名数据
sigData, err := utils.BundleItemSignData(item)
```

---

## 💡 使用场景示例

### 场景 1: 上传文件到 Arweave
```go
package main

import (
    "github.com/permadao/goar"
    "github.com/permadao/goar/schema"
)

func main() {
    // 初始化钱包
    wallet, err := goar.NewWalletFromPath("./keyfile.json", "https://arweave.net")
    if err != nil {
        panic(err)
    }
    
    // 读取文件
    data, _ := os.ReadFile("myfile.txt")
    
    // 添加标签
    tags := []schema.Tag{
        {Name: "Content-Type", Value: "text/plain"},
        {Name: "File-Name", Value: "myfile.txt"},
    }
    
    // 发送数据
    tx, err := wallet.SendData(data, tags)
    if err != nil {
        panic(err)
    }
    
    fmt.Println("Transaction ID:", tx.ID)
}
```

### 场景 2: 大文件并发上传
```go
func uploadLargeFile() {
    wallet, _ := goar.NewWalletFromPath("./keyfile.json", "https://arweave.net")
    
    file, _ := os.Open("large_video.mp4")
    defer file.Close()
    
    tags := []schema.Tag{
        {Name: "Content-Type", Value: "video/mp4"},
    }
    
    ctx := context.Background()
    tx, err := wallet.SendDataConcurrentSpeedUp(
        ctx,
        20,        // 20 个并发
        file,
        tags,
        30,        // 加速 30%
    )
    
    fmt.Println("Upload complete:", tx.ID)
}
```

### 场景 3: 创建和发送 Bundle
```go
func sendBundle() {
    // 初始化 Bundler
    signer, _ := goar.NewSignerFromPath("./keyfile.json")
    bundler, _ := goar.NewBundler(signer)
    
    // 创建多个 Item
    item1, _ := bundler.CreateAndSignItem([]byte("data1"), "", "", nil)
    item2, _ := bundler.CreateAndSignItem([]byte("data2"), "", "", nil)
    
    // 创建 Bundle
    bundle, _ := utils.NewBundle(item1, item2)
    
    // 发送 Bundle
    wallet, _ := goar.NewWalletFromPath("./keyfile.json", "https://arweave.net")
    ctx := context.Background()
    tx, _ := wallet.SendBundleTx(ctx, 10, bundle.Binary, nil)
    
    fmt.Println("Bundle sent:", tx.ID)
}
```

### 场景 4: 使用 ECDSA 签名
```go
func useECDSA() {
    // 创建 ECDSA 签名器
    privateKey := "ccb43edab9f7fd5c24388c43579b9d50ad3c8d94cf5fce9bb0bcb8e6ddb57bce"
    ecSigner, _ := goar.NewEcSigner(privateKey)
    
    fmt.Println("Address:", ecSigner.Address())
    fmt.Println("Owner:", ecSigner.Owner())
    
    // 创建交易
    tx := &schema.Transaction{
        Format: 2,
        Owner:  ecSigner.Owner(),
        // ... 其他字段
    }
    
    // 签名交易
    ecSigner.SignTx(tx)
    
    // 提交交易
    client := goar.NewClient("https://arweave.net")
    client.SubmitTransaction(tx)
}
```

---

## ⚠️ 注意事项

### 1. 网络配置
- 主网：`https://arweave.net`
- 测试网：`https://testnet.arweave.net`
- 建议使用 HTTP 代理提高稳定性

### 2. 费用计算
- AR 单位：1 AR = 10^12 winston
- 使用 `GetTransactionPrice` 预估费用
- 使用 `speedFactor` 加速交易确认

### 3. 大文件处理
- > 100KB 建议使用分块上传
- 使用 `DataReader` 避免内存溢出
- 并发上传提高速度

### 4. Bundle 使用
- Bundle 格式：ANS-104
- Bundle-Format 和 Bundle-Version 标签必需
- 支持嵌套 Bundle

### 5. 错误处理
- `ErrPendingTx`: 交易待确认
- `ErrNotFound`: 交易/区块不存在
- `ErrRequestLimit`: 请求频率限制（429）
- `ErrBadGateway`: 网关错误

---

## 🔗 相关资源

- **官方文档**: https://docs.arweave.org
- **ANS-104 规范**: https://github.com/joshbenaron/arweave-standards/blob/ans104/ans/ANS-104.md
- **arbundles (JS)**: https://github.com/Bundler-Network/arbundles
- **Arweave HTTP API**: https://docs.arweave.org/developers/server/http-api

---

## 📝 总结

**goar** 是一个功能强大且易于使用的 Arweave Go SDK，主要特点：

✅ **功能完整**: 覆盖所有 Arweave 核心功能  
✅ **性能优化**: 支持并发上传、流式处理  
✅ **多签名支持**: RSA、ECDSA、Ed25519  
✅ **Bundle 支持**: 完整的 ANS-104 Bundle 功能  
✅ **错误处理**: 详细的错误类型和状态码  
✅ **易于使用**: 简洁的 API 设计  

适合用于：
- Arweave 数据上传应用
- 去中心化存储网关
- Bundle 打包服务
- AR/PST 转账工具
- 任何需要与 Arweave 交互的 Go 应用
