# Google crc32c vs librdkafka CRC32C 实现对比

## 背景

Google crc32c PR [#74](https://github.com/google/crc32c/pull/74)（commit `578fe78`）与 librdkafka 当前 PR [#5427](https://github.com/confluentinc/librdkafka/pull/5427) 做了同样的事情：在 SSE4.2 硬件 CRC 路径上增加 PCLMULQDQ 折叠优化。但 Google PR 报告的性能提升显著更大。本文档分析差异根源。

## 核心差异：不是优化不够好，而是起点更高

PCLMULQDQ 替换查表折叠的收益取决于查表折叠本身的效率——查表越慢，替换收益越大。

| | Google crc32c (Before) | librdkafka (Before) |
|---|---|---|
| 查表折叠 | `SKIP_BLOCK` — 8×16 nibble 表 | `crc32c_shift` — 4×256 byte 表 |
| 每次折叠操作量 | ~32 次查表 + XOR | 4 次查表 + 3 次 XOR |
| 查表效率 | 每 4-bit nibble 查 16 项表 | 每 8-bit byte 查 256 项表 |
| **PCLMULQDQ 潜在收益** | **替换 ~32 次操作** | **替换 4 次操作** |

librdkafka 的 Mark Adler 原始实现（2013 年）已经对查表折叠做了优化——使用 byte 粒度而非 nibble 粒度。这个设计选择使得我们的 baseline 本身就比 Google 的高效约 **8 倍**，因此 PCLMULQDQ 替换后的相对提升自然更小。

## 逐项对比

### 1. 折叠算法

**Google crc32c — SKIP_BLOCK（查表）**

```c
// 将 32-bit CRC 拆为 8 个 4-bit nibble，逐一查表
crc = skip_table[0][crc & 0xf]
    ^ skip_table[1][(crc >> 4) & 0xf]
    ^ skip_table[2][(crc >> 8) & 0xf]
    ^ skip_table[3][(crc >> 12) & 0xf]
    ^ skip_table[4][(crc >> 16) & 0xf]
    ^ skip_table[5][(crc >> 20) & 0xf]
    ^ skip_table[6][(crc >> 24) & 0xf]
    ^ skip_table[7][(crc >> 28) & 0xf];
```

每次 SKIP_BLOCK：**8 次查表 + 7 次 XOR**，表大小 16 项/表 × 8 = 128 项 = 512 字节。

**Google crc32c — FoldTwoStripes（PCLMULQDQ 后）**

```c
// 两条 PCLMULQDQ + 两条 crc32q + XOR
crc32q(0, pclmul(crc0, K1)) ^ crc2 ^ crc32q(0, pclmul(crc1, K2))
```

每次折叠：**2 次 PCLMULQDQ + 2 次 crc32q + 2 次 XOR**。

**librdkafka — crc32c_shift（查表）**

```c
// 将 32-bit CRC 拆为 4 个 8-bit byte，逐一查表
zeros[0][crc & 0xff]
  ^ zeros[1][(crc >> 8) & 0xff]
  ^ zeros[2][(crc >> 16) & 0xff]
  ^ zeros[3][crc >> 24];
```

每次 crc32c_shift：**4 次查表 + 3 次 XOR**，表大小 256 项/表 × 4 = 1024 项 = 4 KB。

**librdkafka — PCLMULQDQ 折叠（后）**

与 Google 等效：**2 次 PCLMULQDQ + 2 次 crc32q + 2 次 XOR**，但 K 常数通过 GF(2) 矩阵求逆计算。

### 2. 块大小层级

| 层级 | Google (查表) | Google (CLMUL) | librdkafka |
|------|-------------|---------------|------------|
| 1 | 3×5440 = 16,320 B | 3×5376 = 16,128 B | 3×8192 = 24,576 B |
| 2 | 3×1360 = 4,080 B | 3×1344 = 4,032 B | 3×256 = 768 B |
| 3 | 3×336 = 1,008 B | 3×1024 = 3,072 B | — |
| 4 | — | 3×341 = 1,024 B | — |
| 5 | — | 3×85 = 256 B | — |

Google 有 3-5 个层级，librdkafka 只有 2 个。

**影响**：更多层级 = 折叠次数更多 = 折叠开销在总时间中占比更大 = PCLMULQDQ 优化的总收益更大。

在 librdkafka 中，LONG 折叠每 24,576 字节才触发一次，SHORT 折叠每 768 字节一次。在 Google 中，更小的层级间距意味着折叠更频繁。

### 3. 内层循环展开

**Google crc32c — Block0 内层循环**

```c
// 每次迭代处理 64 字节/条带 × 3 = 192 字节
STEP8X3(0);  STEP8X3(1*3*8);  STEP8X3(2*3*8);
STEP8X3(3*3*8);  STEP8X3(4*3*8);  STEP8X3(5*3*8);
STEP8X3(6*3*8);  STEP8X3(7*3*8);
// 共 8 次 STEP8X3，每次 3 条 crc32q
// 每迭代 24 条 crc32q 指令
```

**librdkafka — LONG 内层循环**

```c
// 每次迭代处理 8 字节/条带 × 3 = 24 字节
do {
    crc32q (next+0), crc0;
    crc32q (next+LONG), crc1;
    crc32q (next+2*LONG), crc2;
    next += 8;
} while (next < end);
// 每迭代 3 条 crc32q 指令
```

Google 每次迭代处理 **192 字节**，librdkafka 每次仅 **24 字节**。循环开销差 **8 倍**。

**librdkafka — SHORT+CLMUL 展开后**（`crc32c_hw_clmul_768`）

```asm
// 展开的 inline asm，每次迭代 8 条 crc32q × 3 = 24 条
// 4 次迭代 = 768 字节
```

SHORT 路径的展开已达到 Google 同等级别的密度，但仅覆盖 SHORT 路径。

### 4. 数据预取

| | Google | librdkafka |
|---|---|---|
| LONG 路径 | 三条带全部 prefetch（+256, +BlockSize+256, +2*BlockSize+256） | **无 prefetch** |
| SHORT 路径 | 三条带全部 prefetch | 一条带 prefetch（+512） |

LONG 路径缺少 prefetch 是 librdkafka 的一个可改进点。在 16MB 以上的大缓冲区上，缓存缺失可能成为瓶颈。

### 5. 折叠常数的计算方式

| | Google | librdkafka |
|---|---|---|
| 常数来源 | 编译期常量（`tools/compute_folding_constants.cc` 预计算） | 运行时 GF(2) 矩阵求逆（`crc32c_compute_k_clmul`） |
| 初始化开销 | 零 | 32×32 高斯消元（一次性，<1μs） |
| 灵活性 | 块大小固定 | 块大小可动态调整 |

librdkafka 的运行时求逆在 init 时仅执行一次，开销可忽略。编译期常量方案在代码可读性上更优，但需要额外的代码生成工具。

### 6. 软件回退路径

| | Google | librdkafka |
|---|---|---|
| 并行度 | 4 路 stride (16 字节/次) | slice-by-8 (8 字节/次) |
| 预取 | 有 | 无 |
| 吞吐 (1MB) | ~2.5 GB/s（估计） | ~1.6 GB/s（实测） |

Google 的软件路径也优于 librdkafka，但这不影响硬件路径的对比。

## 结论

Google crc32c PR #74 的性能提升更大，是因为：

1. **起点更低**：nibble 级查表折叠（~32 次操作/折叠）vs 我们的 byte 级查表（4 次操作/折叠），PCLMULQDQ 替换的相对收益差 8 倍
2. **折叠频率更高**：5 层级 vs 2 层级，折叠操作在总时间中占比更大
3. **循环展开更激进**：64 字节/条带/迭代 vs 8 字节/条带/迭代，折叠占比更高
4. **全路径 prefetch**：LONG 路径也有数据预取

**librdkafka 的优化不是做得不够**——Mark Adler 2013 年的设计选择（byte 查表、大块尺寸）已经将折叠开销压缩到极致。PCLMULQDQ 替换后，我们获得了 1-12% 的提升（4KB 处最大 12%），这在 IPC 2.0 的 crc32q 天花板下是合理的。

要进一步突破，需要借鉴 Google 的架构改进（更多层级、更密集展开、全路径 prefetch），或者走全 PCLMULQDQ 路线（`crc32c_pcl.c`）。
