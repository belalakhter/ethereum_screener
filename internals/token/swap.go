package token

import "math/big"

func decodeSwapAmounts(data []byte) (*big.Int, *big.Int, *big.Int, *big.Int, bool) {
	if len(data) < 128 {
		return nil, nil, nil, nil, false
	}

	return new(big.Int).SetBytes(data[0:32]),
		new(big.Int).SetBytes(data[32:64]),
		new(big.Int).SetBytes(data[64:96]),
		new(big.Int).SetBytes(data[96:128]),
		true
}

func classifySwapTrade(config *TransactionStreamConfig, amount0In, amount1In, amount0Out, amount1Out *big.Int) (string, *big.Int, *big.Int, bool) {
	tradeType := "Swap"
	tokenAmountRaw := big.NewInt(0)
	wrappedAmountRaw := big.NewInt(0)

	if config.WrappedIsToken0 {
		switch {
		case amount0In.Sign() > 0 && amount1Out.Sign() > 0:
			tradeType = "Buy"
			wrappedAmountRaw = new(big.Int).Set(amount0In)
			tokenAmountRaw = new(big.Int).Set(amount1Out)
		case amount1In.Sign() > 0 && amount0Out.Sign() > 0:
			tradeType = "Sell"
			tokenAmountRaw = new(big.Int).Set(amount1In)
			wrappedAmountRaw = new(big.Int).Set(amount0Out)
		}
	} else {
		switch {
		case amount1In.Sign() > 0 && amount0Out.Sign() > 0:
			tradeType = "Buy"
			wrappedAmountRaw = new(big.Int).Set(amount1In)
			tokenAmountRaw = new(big.Int).Set(amount0Out)
		case amount0In.Sign() > 0 && amount1Out.Sign() > 0:
			tradeType = "Sell"
			tokenAmountRaw = new(big.Int).Set(amount0In)
			wrappedAmountRaw = new(big.Int).Set(amount1Out)
		}
	}

	if tokenAmountRaw.Sign() <= 0 || wrappedAmountRaw.Sign() <= 0 {
		return "", nil, nil, false
	}

	return tradeType, tokenAmountRaw, wrappedAmountRaw, true
}
