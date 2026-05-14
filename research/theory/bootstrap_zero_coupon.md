# Bootstrapping Zero-Coupon Rates from Swap Rates

This document provides a mathematical and practical walkthrough of how to derive **discount factors** and **zero-coupon spot rates** from a set of **par swap rates**.

---

## 1) Setup & Notation

- Payment dates: \(0=t_0 < t_1 < \dots < t_N=T\) on a regular grid.  
- Year-fraction (accrual) for period \(i\): \(\alpha_i\) (often constant \(\alpha\), e.g., \(\alpha=0.5\) for semiannual).  
- Par swap rate to maturity \(T_N\): \(S_N\).  
- Discount factor: \(P(0,t_i)\) (PV of 1 paid at \(t_i\)).  
- Zero-coupon rate to \(T\).

---

## 2) Pricing Identities

### (A) Fixed leg present value
\[
\text{PV}_{\text{fixed}}(S_N, T_N)
= S_N \sum_{i=1}^{N} \alpha_i P(0,t_i).
\tag{1}
\]

### (B) Floating leg present value
\[
\text{PV}_{\text{float}}(T_N) = 1 - P(0,T_N).
\tag{2}
\]

### (C) Par swap condition
\[
S_N \sum_{i=1}^{N} \alpha_i P(0,t_i) = 1 - P(0,T_N).
\tag{3}
\]

---

## 3) The Bootstrap Recursion

Splitting (3) into known and unknown terms:
\[
S_N \!\left( \sum_{i=1}^{N-1} \alpha_i P(0,t_i) + \alpha_N P(0,t_N) \right)
= 1 - P(0,t_N).
\]

Solve for \(P(0,t_N)\):
\[
P(0,t_N) = \frac{1 - S_N \sum_{i=1}^{N-1}\alpha_i P(0,t_i)}{1 + S_N \alpha_N}.
\tag{4}
\]

Special case \(N=1\):
\[
P(0,t_1) = \frac{1}{1 + S_1 \alpha_1}.
\tag{5}
\]

---

## 4) Zero-Coupon Rates from Discount Factors

Given \(P(0,T)\), compute:

- **Continuously compounded:**
\[
r_{\text{cont}}(T) = -\frac{\ln P(0,T)}{T}.
\tag{6}
\]

- **Annually compounded:**
\[
r_{\text{ann}}(T) = P(0,T)^{-1/T} - 1.
\tag{7}
\]

- **m-times per year:**
\[
r_m(T) = m\!\left( P(0,T)^{-1/(mT)} - 1 \right).
\tag{8}
\]

- **Simple:**
\[
r_{\text{simple}}(T) = \frac{1}{P(0,T)} - 1 \;\Big/\, T.
\tag{9}
\]

---

## 5) Worked Example (Semiannual Payments)

Par swap rates (decimals), semiannual payments (\(\alpha=0.5\)):

- Maturities: 0.5y, 1.0y, 1.5y, 2.0y  
- Swap rates: 0.0300, 0.0320, 0.0335, 0.0350

### Step 1: T=0.5
\[
P(0,0.5) = \frac{1}{1 + 0.0300 \times 0.5} = 0.98522
\]

### Step 2: T=1.0
\[
P(0,1.0) = \frac{1 - 0.0320 \times (0.5 \times 0.98522)}{1 + 0.0320 \times 0.5} = 0.96874
\]

### Step 3: T=1.5
\[
P(0,1.5) = \frac{1 - 0.0335 \times (0.5(0.98522+0.96874))}{1 + 0.0335 \times 0.5} = 0.95134
\]

### Step 4: T=2.0
\[
P(0,2.0) = \frac{1 - 0.0350 \times (0.5(0.98522+0.96874+0.95134))}{1 + 0.0350 \times 0.5} \approx 0.93283
\]

### Convert to Zero Rates

| T (y) | P(0,T)  | r_cont | r_ann |
|------:|---------:|--------:|-------:|
| 0.5   | 0.98522  | 0.02978 | 0.03023 |
| 1.0   | 0.96874  | 0.03176 | 0.03227 |
| 1.5   | 0.95134  | 0.03326 | 0.03382 |
| 2.0   | 0.93283  | 0.03476 | 0.03538 |

---

## 6) Code Mapping

- **Bootstrap step (eq. 4):**
```python
P_N = (1.0 - S_N * sum_known) / (1.0 + S_N * alpha_N)
```

- **Zero rate conversion (eq. 6–8):**
```python
r = -np.log(P) / T          # continuous
r = P ** (-1.0 / T) - 1.0   # annual
```

---

## 7) Practical Notes

- Use actual \(\alpha_i\) for irregular coupons.  
- Ensure maturities align with the coupon grid.  
- Discount factors should be positive and decreasing.  
- In modern markets, OIS discounting + tenor-dependent floating legs are common.

---
