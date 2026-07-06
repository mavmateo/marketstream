from ai.signals.trend_detector import detect, _classify

def make_tick(**overrides):
    base = {
          "symbol": "BTCUSDT", "market": "crypto",
        "timestamp": "2026-07-02T14:00:00Z",
        "open": 100.0, "high": 110.0, "low": 90.0,
        "close": 108.0, "price": 108.0, "volume": 1000.0,
        "price_range": 20.0, "body_size": 8.0,
        "upper_shadow": 2.0, "lower_shadow": 10.0,
        "is_bullish": True,
    }

    return {**base, **overrides}


def test_strong_bullish():
    tick = make_tick(body_size=16.0, price_range=20.0, is_bullish=True)
    direction, confidence = _classify(tick)
    assert direction == "STRONG_BULLISH"
    assert confidence > 0.70

def test_neutral_flat_candle():
    tick = make_tick(price_range=0.0)
    direction, confidence = _classify(tick)
    assert direction == "NEUTRAL"
    assert confidence == 0.0

def test_detect_returns_correct_keys():
    signal = detect(make_tick())
    assert "time" in signal
    assert "direction" in signal
    assert "confidence" in signal
    assert "predicted_price" in signal