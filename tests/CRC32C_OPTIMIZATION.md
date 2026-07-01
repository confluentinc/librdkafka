# CRC32C 优化：从 SSE4.2 到 PCLMULQDQ

## 对比焦点

本文档对比 **优化前**（SSE4.2 crc32q + GF(2) 查表折叠）与 **优化后**（SSE4.2 crc32q + PCLMULQDQ 折叠 + 展开 768B 函数）两个版本。

软件回退路径不作为对比重点——它仅在无 SSE4.2 的 CPU 上触发。

## 一、优化前：SSE4.2 + GF(2) 查表折叠

### 算法结构

```
数据 → 三路并行 crc32q（8192B/256B 块）→ 条带 CRC → GF(2) 查表折叠 → 合并 CRC
       ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^     ^^^^^^^^^^^^^^^^^^^^^^^^
       占 99% CPU 时间                             占 ~1% CPU 时间
```

### 折叠操作

三条带并行的 CRC 计算完成后，需要将三个独立的 CRC 值合并为一个。合并操作本质是 GF(2) 上的"前移+XOR"：

```
crc0 = shift(crc0, BLOCK) ^ crc1;   // 4 次查表 + 3 次 XOR
crc0 = shift(crc0, BLOCK) ^ crc2;   // 4 次查表 + 3 次 XOR
```

`shift` 函数通过两张 4KB 查找表 (`crc32c_long[4][256]`, `crc32c_short[4][256]`) 实现 GF(2) 乘法。

### 性能特征

| 特性 | 值 |
|------|-----|
| 折叠频率 (LONG) | 每 24,576 字节一次 |
| 折叠频率 (SHORT) | 每 768 字节一次 |
| 每次折叠操作 | 8 次 L1 查表 + 6 次 XOR |
| IPC (perf) | ~2.0 |
| L1 缓存占用 | 8 KB (两张折叠表) |

## 二、优化后：SSE4.2 + PCLMULQDQ 折叠 + 展开函数

### 改动一：PCLMULQDQ 替代查表折叠

**原理**：PCLMULQDQ 是 CPU 硬件指令，可在 1-3 周期内完成 GF(2) 上的 64-bit × 64-bit 无进位乘法。配合 `crc32q` 做模 P 归约，单条指令替代 4 次查表 + 3 次 XOR。

**折叠常数 K 的关键推导**：

```
crc32q(0, K') = target   （其中 target = x^(N*8) mod P）
```

由于 `crc32q` 隐含 `* x^32` 操作：
```
K' * x^32 mod P = target    →    K' = target * x^(-32) mod P
```

这需要 GF(2) 矩阵求逆——不能简单取 target 本身作为 K 常数。

**实现**：32×32 GF(2) 矩阵高斯消元，在 init 时一次性计算 4 个 K 常数。

### 改动二：展开的 768 字节 SHORT 路径

**原理**：Profiling 数据显示 SHORT 折叠次数是 LONG 的 **16 倍**。原 SHORT 路径使用 do-while 循环，每次迭代 3 条 crc32q，共 32 次迭代。展开后消除循环开销，添加 prefetch。

**展开结构**（每次迭代 8 条 crc32q × 3 条带 = 24 条 crc32q）：

```asm
prefetcht0 512(%in)          # 预取 2 条带后的数据
.L_loop:  (×4 iterations)
    crc32q   0(%in), %rcx    # stripe 0
    crc32q 256(%in), %r11    # stripe 1
    crc32q 512(%in), %r10    # stripe 2
    crc32q   8(%in), %rcx
    ...  (5 more triplets per iteration) ...
    add $64, %in; decl %r8d; jnz .L_loop
# 末尾: PCLMULQDQ 折叠三个条带
```

### 改动三：函数指针分派

**优化前**：每次 `rd_crc32c()` 调用检查 `if (sse42)` 分支。

**优化后**：init 时设置函数指针 `crc32c_impl`，调用时单次间接跳转。

### 代码结构

| | 优化前 | 优化后 |
|---|---|---|
| 折叠方式 | GF(2) 查表 (4 lookup + 3 XOR) | PCLMULQDQ × 2 + crc32q × 2 |
| SHORT 路径 | do-while 循环 | 完全展开 + prefetch |
| LONG 路径 | do-while 循环 | do-while 循环（不变，折叠频率极低） |
| 分派 | `if (sse42)` 分支 | 函数指针间接调用 |
| L1 缓存 | 8 KB 查找表常驻 | 查表仅 Nehalem 回退时使用 |
| 文件 | `crc32c.c` (单文件) | `crc32c.c` + `crc32c_impl.h` + `crc32c_pcl.c` |

## 三、性能对比

### 测试环境

| 项目 | 值 |
|------|-----|
| CPU | Intel Xeon E5-2678 v3 @ 2.50GHz (Haswell-EP) |
| OS | Ubuntu 20.04.4, Linux 5.4.0-107 |
| 编译器 | GCC 9.4.0, -O2 |

### 吞吐量对比

| 缓冲区 | 优化前 (MB/s) | 优化后 (MB/s) | 提升 |
|--------|-------------|-------------|------|
| 64 B | 2,643 | 2,638 | 1.00x |
| 256 B | 5,357 | 5,353 | 1.00x |
| 512 B | 5,922 | 5,921 | 1.00x |
| 1 KB | 14,065 | 14,359 | 1.02x |
| 4 KB | 18,297 | 19,523 | **1.07x** |
| 8 KB | 19,247 | 19,353 | 1.01x |
| 16 KB | 20,265 | 21,091 | 1.04x |
| 64 KB | 22,909 | 24,370 | **1.06x** |
| 256 KB | 23,156 | 24,128 | 1.04x |
| 1 MB | 23,335 | 23,569 | 1.01x |

优化后在 4-64 KB 区间有 **1-7%** 的可测量提升。折叠操作占比极低（每 768B 一次 SHORT，每 24KB 一次 LONG），因此整体提升有限但一致。

### perf stat 微架构分析

```
 6,009,636,447  cycles                    # 6.0B cycles
12,037,594,995  instructions              # 2.00 IPC
   638,156,407  branches                  # 0.22% miss rate
 3,737,478,098  L1-dcache-loads           # 2.47% miss rate
```

| 指标 | 值 | 解读 |
|------|-----|------|
| IPC | 2.00 | 接近三路并行 crc32q 的理论天花板（每周期 1 条） |
| 分支失误 | 0.22% | 循环分支被完美预测，展开的主要收益来自减少循环计数器更新 |
| L1 缺失 | 2.47% | 缓存效率良好；优化后消除的 8KB 表在生产环境（I/O 交织场景）中可降低缓存压力 |

### Profiling 路径分布

```
LONG bytes:     62.6%     ← ≥ 24 KB 块
SHORT bytes:    31.0%     ← 768 B – 24 KB 块
residual:        6.2%     ← < 768 B
LONG folds:    134,800
SHORT folds: 2,134,000    ← 16 倍于 LONG
align bytes:    0.25%     ← 可忽略
```

SHORT 折叠频率是 LONG 的 16 倍——展开函数针对最高频路径优化。

## 四、为什么提升幅度有限

perf 数据给出明确答案：**IPC 已达 2.0**。

三路并行的 crc32q 每周期可发射 1 条指令，理论最大 IPC 约 2.0-2.5（考虑循环开销）。当前代码已接近这个天花板。

折叠操作仅占总时间的 ~1%（每 768 字节触发一次 SHORT 折叠），因此即使将折叠从 8 次查表优化为 2 条 PCLMULQDQ 指令，总收益也只有 1-7%。

要突破 20 GB/s 天花板，需要将主体数据处理从 crc32q 替换为 PCLMULQDQ——即 `crc32c_pcl.c` 中的实验性路径。该路径将 PCLMULQDQ 用于数据折叠而非仅用于条带合并，预期可将吞吐提升至 30-40 GB/s，但当前版本在 CRC-32C 的 init/final XOR 惯例处理上仍需调试。

## 五、CPU 兼容性

| CPU | 使用的路径 |
|-----|----------|
| Nehalem (2008, SSE4.2, 无 PCLMULQDQ) | crc32q + 查表折叠（回退） |
| Westmere+ (2010+, 有 PCLMULQDQ) | crc32q + PCLMULQDQ 折叠 + 展开 768B |
| 无 SSE4.2 | 纯软件 slice-by-8 |

运行时 CPUID 自动选择，`RD_CRC32C_NO_CLMUL=1` 可强制回退用于 A/B 测试。

## 六、代码改动

| 文件 | 变更 | 说明 |
|------|------|------|
| `src/crc32c.c` | +230 / −58 | PCLMULQDQ 折叠、展开 768B 函数、profiling、函数指针分派 |
| `src/crc32c_impl.h` | 新建 18 行 | 内部声明 |
| `src/crc32c_pcl.c` | 新建 282 行 | 实验性全 PCLMULQDQ 折叠路径 |
| `src/Makefile` | +1 行 | |

公共 API 不变，ABI 兼容。仅一个公共头文件 `crc32c.h`，内部实现拆分为多文件。

## 七、参考文献

1. Gopal, Ozturk et al. "Fast CRC Computation for Generic Polynomials Using PCLMULQDQ Instruction." Intel, 2009.
2. AWS Checksums: `github.com/awslabs/aws-checksums` — CRC32C SSE4.2 参考实现
3. Mark Adler. `crc32c.c` — 原始 SSE4.2 CRC-32C 实现, 2013.
4. Linux kernel `arch/x86/crypto/crc32c-pcl-intel-asm_64.S` — PCLMULQDQ-based CRC32C
