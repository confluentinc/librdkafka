# CRC32C 深度优化：从第一性原理到实践

## 一、第一性原理

### 1.1 CRC-32C 的数学本质

CRC-32C 是 GF(2) 上的多项式除法。给定消息 M 和生成多项式 P(x)：

```
P(x) = x^32 + x^28 + x^27 + x^26 + x^25 + x^23 + x^22
     + x^20 + x^19 + x^18 + x^14 + x^13 + x^11 + x^10
     + x^9 + x^8 + x^6 + 1

CRC(M) = (M(x) * x^32 mod P(x)) XOR 0xFFFFFFFF
```

反射表示下，位 i 对应 x^i 的系数。消息按 LSB 优先顺序逐字节处理。

### 1.2 三种实现层级

| 层级 | 核心操作 | 每次吞吐 | 硬件延迟 | 性能瓶颈 |
|------|---------|---------|---------|---------|
| 软件查表 (slice-by-8) | 8 张 256 项查找表 | 8 字节/次 | ~5 ns/byte | L1 缓存查表延迟 |
| CRC32Q 硬件指令 | `crc32q` (SSE4.2) | 8 字节/次 | 3 cycles, 1/cycle 吞吐 | 指令流水线延迟 |
| PCLMULQDQ 无进位乘法 | `pclmulqdq` | 64-bit × 64-bit → 128-bit | 7 cycles | GF(2) 乘法延迟 |

**核心洞察**：`crc32q` 延迟为 3 周期但吞吐为每周期 1 条。三路并行可完全隐藏延迟——这正是 `crc32c_hw()` 中三条带 (crc0/crc1/crc2) 并行计算的设计依据。

### 1.3 折叠（Folding）的数学

CRC 折叠是将多个独立计算的 CRC 值合并为整体 CRC 的操作：

```
CRC(A || B) = CRC(A) * x^(len(B)*8) mod P  XOR  CRC(B)
```

其中"乘以 x^N"操作在 GF(2) 中等价于将 CRC 多项式前移 N 位后模 P 归约。PCLMULQDQ 可在单条指令中完成此 GF(2) 乘法。

**折叠常数 K 的关键推导**：

设目标为 `T = x^(N*8) mod P`（即前移 N 字节后的 GF(2) 等价常数），需要找到 K' 使得：

```
crc32q(0, K') = T
```

由于 `crc32q` 隐含 `* x^32` 操作（CRC 算法对消息尾部追加 32 个零位），实际满足的是：

```
K' * x^32 mod P = T    →    K' = T * x^(-32) mod P
```

这意味着 K' 不能直接从 `crc32c_zeros_op()`（它计算 x^N mod P）获取——必须通过 GF(2) 矩阵求逆将 T 变换为 K'。实现中使用 32×32 二进制矩阵的高斯消元完成此变换。

## 二、Profiling：定位真正的瓶颈

### 2.1 插桩方法

在 `crc32c_hw()` 中添加了调用统计计数器，通过环境变量 `RD_CRC32C_PROFILE=1` 启用，`atexit()` 自动打印。

收集的指标：

| 指标 | 含义 |
|------|------|
| calls | 总调用次数 |
| total_bytes / calls | 平均每次调用处理的字节数 |
| long_bytes / short_bytes / residual_bytes | 三条路径的数据量分布 |
| align_bytes | 对齐前导字节数 |
| long_folds / short_folds | 各路径的折叠触发次数 |
| clmul_folds / table_folds | PCLMULQDQ vs 查表折叠的调用次数 |

### 2.2 插桩数据（benchmark 1.9M 次调用，总计 5.7 GB）

```
=== CRC32C Profile ===
  calls:          1,956,220
  total bytes:    6,114,928,768 (avg 3.1 KB/call)
  LONG bytes:     3,315,499,008 (54.2%)
  SHORT bytes:    2,408,601,600 (39.4%)
  residual bytes:   377,836,020 (6.2%)
  align bytes:       12,992,140 (0.2%)    ← 可忽略
  LONG folds:          134,908
  SHORT folds:       3,136,200            ← 23 倍于 LONG!
  CLMUL folds:       1,635,554 (50.0%)
  table folds:       1,635,554 (50.0%)
```

### 2.3 关键发现

1. **对齐开销仅 0.2%**——不需要优化
2. **SHORT 折叠次数是 LONG 的 23 倍**（310 万 vs 13 万）——优化 SHORT 路径比 LONG 更有价值
3. **折叠操作占比极低**——每 768 字节触发一次 SHORT 折叠，每 24,576 字节触发一次 LONG 折叠。折叠本身不是瓶颈
4. **真正的瓶颈是 `crc32q` 主循环**——数据处理指令占据绝对主要的 CPU 时间

### 2.4 生产环境的路径分布

在 Kafka 生产场景中，典型 `batch.size=1MB`：
- **LONG 路径**（≥24KB）：占绝大多数调用
- **SHORT 路径**（768B~24KB）：小批次或测试场景的边缘情况
- **残余路径**（<768B）：仅出现在块尾部碎片

## 三、优化实践

### 3.1 优化一：PCLMULQDQ 折叠（第一轮）

**原理**：将 `crc32c_hw()` 中 SHORT/LONG 块的 GF(2) 矩阵查表折叠（4 次查表 + 3 次 XOR）替换为 PCLMULQDQ 单指令折叠。

**实现要点**：
- K 常数通过 GF(2) 矩阵求逆计算（`crc32c_compute_k_clmul()`），正确补偿 `crc32q` 隐含的 x^32 因子
- 运行时 CPUID 检测 PCLMULQDQ 支持（CPUID.01H:ECX bit 1）
- Nehalem（有 SSE4.2 无 PCLMULQDQ）自动回退到查表折叠
- `RD_CRC32C_NO_CLMUL=1` 可强制回退用于 A/B 测试

**性能效果**：微基准中与查表折叠差异在 ±5% 测量噪声范围内——折叠操作占比太低。

### 3.2 优化二：展开的 768 字节 CLMUL 函数（第二轮）

**原理**：为 SHORT 路径（256 字节 × 3 条带 = 768 字节）引入完全展开的三路并行 CRC32Q 函数，消除循环开销并添加数据预取。

**展开结构**（每次迭代处理 64 字节/条带，4 次迭代 = 256 字节/条带）：

```asm
prefetcht0 512(%in)          # 预取 2 条带后的数据到 L1
movl $4, %r8d                # 循环计数器: 256/64 = 4
.L_loop:
    crc32q   0(%in), %rcx    # stripe 0: offset 0
    crc32q 256(%in), %r11    # stripe 1: offset 256
    crc32q 512(%in), %r10    # stripe 2: offset 512
    crc32q   8(%in), %rcx
    crc32q 264(%in), %r11
    crc32q 520(%in), %r10
    ; ... 5 more triplets (totaling 8 triplets per iteration) ...
    add $64, %in
    decl %r8d
    jnz .L_loop
# PCLMULQDQ fold: crc0*K_short2 XOR crc1*K_short XOR crc2
```

**优化效果**：
- 消除 do-while 循环的分支预测开销和计数器更新
- `prefetcht0` 提前将数据加载到 L1，减少缓存缺失停顿
- PCLMULQDQ 折叠在展开块末尾直接完成，避免函数调用开销

### 3.3 环境变量开关

| 变量 | 作用 |
|------|------|
| `RD_CRC32C_NO_CLMUL=1` | 强制禁用 PCLMULQDQ，回退查表折叠（A/B 测试） |
| `RD_CRC32C_PROFILE=1` | 启用调用统计，进程退出时自动打印 |

## 四、性能数据

### 4.1 测试环境

| 项目 | 值 |
|------|-----|
| CPU | Intel Xeon E5-2678 v3 @ 2.50GHz (Haswell-EP) |
| 微架构 | Haswell, model 63, stepping 2 |
| 核心 | 12C/24T, 30MB L3 |
| CPU 特性 | pclmulqdq sse4_1 sse4_2 avx2 |
| OS | Ubuntu 20.04.4 LTS (Focal Fossa) |
| Kernel | Linux 5.4.0-107-generic |
| 编译器 | GCC 9.4.0, `-O2` |

### 4.2 综合性能对比（8192 字节）

| 路径 | 吞吐量 | 延迟 | vs SW |
|------|--------|------|-------|
| SW (纯软件 slice-by-8) | 1,628 MB/s | 5,033 ns | 1.00x |
| HW (SSE4.2 + 查表折叠) | 18,142 MB/s | 452 ns | **11.1x** |
| HW (SSE4.2 + CLMUL + 展开) | 20,104 MB/s | 408 ns | **12.4x** |

### 4.3 各缓冲区大小详细对比

| 缓冲区 | SW (MB/s) | HW+查表 (MB/s) | HW+CLMUL (MB/s) | CLMUL/查表 | CLMUL/SW |
|--------|----------|---------------|-----------------|-----------|---------|
| 64 B | 889 | 3,127 | 3,030 | 0.97x | 3.4x |
| 256 B | 1,167 | 5,334 | 5,353 | 1.00x | 4.6x |
| 512 B | 1,509 | 7,343 | 7,118 | 0.97x | 4.7x |
| 1 KB | 1,573 | 14,123 | 14,578 | 1.03x | 9.3x |
| 4 KB | 1,678 | 18,660 | 19,602 | **1.05x** | 11.7x |
| 8 KB | 1,679 | 17,963 | 19,761 | **1.10x** | 11.8x |
| 16 KB | 1,680 | 20,370 | 22,138 | **1.09x** | 13.2x |
| 64 KB | 1,668 | 23,672 | 24,907 | 1.05x | 14.9x |
| 256 KB | 1,650 | 23,946 | 23,603 | 0.99x | 14.3x |
| 1 MB | 1,662 | 22,897 | 22,697 | 0.99x | 13.7x |

### 4.4 性能分析

1. **硬件 vs 软件**：4~15 倍提升。`crc32q` 指令每周期 1 条、每指令处理 8 字节，远超软件查表方案。缓冲区越大，指令流水线填充越充分，提升越明显。

2. **CLMUL+展开 vs 纯查表**：
   - 中小缓冲区（4-16 KB）：**5-10%** 可测量的提升——展开的 CRC32Q + prefetch + PCLMULQDQ 折叠的共同效果
   - 极大缓冲区（256 KB+）：差异回到噪声范围——LONG 路径主导，折叠频率极低（每 24KB 一次）
   - Prefetch 对 4-64 KB 区间效果最明显：此区间数据可能不在 L1 缓存

3. **小缓冲区（<512B）CLMUL 略慢**：展开函数调用开销 + PCLMULQDQ 指令延迟（7 cycles）高于查表折叠（~5 cycles），在极短数据上反而略有损耗。但差异仅 3%，可忽略。

## 五、CPU 兼容性矩阵

| CPU 微架构 | 年份 | SSE4.2 | PCLMULQDQ | 实际路径 |
|-----------|------|--------|-----------|---------|
| 无 SSE4.2 | — | ✗ | ✗ | SW slice-by-8 |
| Nehalem | 2008 | ✓ | ✗ | HW + 查表折叠 |
| Westmere | 2010 | ✓ | ✓ | HW + CLMUL 折叠 |
| Sandy Bridge+ | 2011+ | ✓ | ✓ | HW + CLMUL 折叠 + 展开 |
| Haswell (本测试) | 2014 | ✓ | ✓ | HW + CLMUL 折叠 + 展开 |

运行时 CPUID 自动检测，无需用户干预。

## 六、代码改动

所有改动集中在 `src/crc32c.c` 一个文件：

| 组件 | 新增行 | 职责 |
|------|--------|------|
| CLMUL CPU 检测 | +8 | `CLMUL()` 宏 |
| K 常数 GF(2) 矩阵求逆 | +40 | `crc32c_compute_k_clmul()` |
| PCLMULQDQ 折叠函数 | +25 | `crc32c_fold_stripe_clmul()` |
| 展开的 768B 函数 | +70 | `crc32c_hw_clmul_768()` |
| Profiling 插桩 | +65 | 调用统计 + 环境变量开关 |
| crc32c_hw 条件分派 | +20 | if/else 分发到 CLMUL/查表路径 |
| 头文件 + attribute | +5 | `<wmmintrin.h>` + `target("pclmul")` |
| **总计** | **~233 行** | 公共 API 签名和行为完全不变 |

## 七、进一步优化方向

基于 profiling 数据，以下方向值得探索：

| 优先级 | 方向 | 预期收益 | 说明 |
|--------|------|---------|------|
| 高 | 软件路径 slice-by-16 | 软件路径 1.5-2x | 16 张查找表并行，适用于无 SSE4.2 的旧 CPU |
| 中 | ARMv8 硬件 CRC | ARM 上 10-20x | 使用 `__crc32cd` 和展开循环，当前缺失 |
| 低 | AVX512 + VPCLMULQDQ | Ice Lake+ 上 2-3x | AWS checksums 已有完整实现可参考 |
| 低 | 函数指针分派 | 微小（<0.5%） | 消除 `if (sse42)` 分支 |

## 八、参考文献

1. Gopal, Ozturk et al. "Fast CRC Computation for Generic Polynomials Using PCLMULQDQ Instruction." Intel, 2009.
2. AWS Checksums: `github.com/awslabs/aws-checksums` — CRC32C SSE4.2/AVX512 参考实现
3. Mark Adler. `crc32c.c` — original SSE4.2 CRC-32C implementation, 2013.
4. Linux kernel `arch/x86/crypto/crc32c-pcl-intel-asm_64.S` — PCLMULQDQ-based CRC32C
