(function () {
    const maxTransactionRows = 12;
    const maxChartCandles = 300;
    const chartCandleIntervalSeconds = 10 * 60;
    const chartGapFillIntervalMs = 30 * 1000;
    const pairStorageKey = "ethscan:lastPair";
    const useNativeChartRenderer = true;
    const streamReconnectMinDelayMs = 2000;
    const streamReconnectMaxDelayMs = 30000;
    let activeEventSource = null;
    let activeStreamURL = "";
    let activeStreamReconnectTimer = null;
    let activeStreamReconnectDelayMs = streamReconnectMinDelayMs;
    let activeChart = null;
    let activeCandleSeries = null;
    let activeVolumeSeries = null;
    let activeFallbackChartEl = null;
    let activeCandles = new Map();
    let activeChartResizeObserver = null;
    let activeChartGapFillTimer = null;
    let activeChartHistoryRequest = null;
    let activeChartHistoryLoaded = false;
    let pendingChartTicks = [];
    let activePricePrecision = null;

    function closeStream(reason) {
        if (activeStreamReconnectTimer) {
            window.clearTimeout(activeStreamReconnectTimer);
            activeStreamReconnectTimer = null;
        }

        if (!activeEventSource) {
            return;
        }

        console.info("[sse] closing", activeStreamURL, reason || "");
        activeEventSource.close();
        activeEventSource = null;
        activeStreamURL = "";
    }

    function scheduleStreamReconnect() {
        if (activeStreamReconnectTimer) {
            return;
        }

        const delay = activeStreamReconnectDelayMs;
        activeStreamReconnectDelayMs = Math.min(
            activeStreamReconnectDelayMs * 2,
            streamReconnectMaxDelayMs,
        );
        activeStreamReconnectTimer = window.setTimeout(function () {
            activeStreamReconnectTimer = null;
            connectTransactionStream();
        }, delay);
    }

    function updateStreamStatus(message, isError) {
        const statusEl = document.querySelector("[data-sse-status]");
        if (!statusEl) {
            return;
        }

        statusEl.classList.remove("text-blue-100/85", "text-green-300", "text-red-300");
        if (!isError) {
            statusEl.textContent = "";
            statusEl.classList.add("hidden");
            return;
        }

        statusEl.textContent = message;
        statusEl.classList.remove("hidden");
        statusEl.classList.add("text-red-300");
    }

    function updateChartStatus(message, tone) {
        const statusEl = document.querySelector("[data-chart-status]");
        if (!statusEl) {
            return;
        }

        statusEl.textContent = message;
        statusEl.classList.remove("text-blue-100/75", "text-green-300", "text-red-300");
        if (tone === "success") {
            statusEl.classList.add("text-green-300");
            return;
        }
        if (tone === "error") {
            statusEl.classList.add("text-red-300");
            return;
        }

        statusEl.classList.add("text-blue-100/75");
    }

    function normalizeAddressInput(value) {
        return String(value || "").trim();
    }

    function isLikelyAddress(value) {
        return /^0x[a-fA-F0-9]{40}$/.test(normalizeAddressInput(value));
    }

    function pairValuesMatch(left, right) {
        return normalizeAddressInput(left).toLowerCase() === normalizeAddressInput(right).toLowerCase();
    }

    function getPairInput() {
        return document.getElementById("pairInput");
    }

    function getPairFromURL() {
        try {
            return normalizeAddressInput(new URL(window.location.href).searchParams.get("pair"));
        } catch (error) {
            return "";
        }
    }

    function getStoredPair() {
        try {
            return normalizeAddressInput(window.localStorage.getItem(pairStorageKey));
        } catch (error) {
            return "";
        }
    }

    function storePair(pair) {
        const normalized = normalizeAddressInput(pair);
        if (!isLikelyAddress(normalized)) {
            return;
        }

        try {
            window.localStorage.setItem(pairStorageKey, normalized);
        } catch (error) {
            console.warn("[state] unable to store pair", error);
        }
    }

    function updatePairURL(pair) {
        const normalized = normalizeAddressInput(pair);
        if (!isLikelyAddress(normalized) || !window.history || !window.history.replaceState) {
            return;
        }

        try {
            const url = new URL(window.location.href);
            url.searchParams.set("pair", normalized);
            window.history.replaceState({}, "", url);
        } catch (error) {
            console.warn("[state] unable to update pair URL", error);
        }
    }

    function getRenderedPair() {
        const resultEl = document.getElementById("result");
        const pairEl = resultEl ? resultEl.querySelector("[data-resolved-pair]") : null;
        return pairEl ? normalizeAddressInput(pairEl.getAttribute("data-resolved-pair")) : "";
    }

    function restorePairInput() {
        const input = getPairInput();
        const restoredPair = getPairFromURL() || getStoredPair() || normalizeAddressInput(input ? input.value : "");
        if (input && restoredPair && !pairValuesMatch(input.value, restoredPair)) {
            input.value = restoredPair;
        }

        return restoredPair;
    }

    function persistSubmittedPair() {
        const input = getPairInput();
        const submittedPair = normalizeAddressInput(input ? input.value : "");
        if (!isLikelyAddress(submittedPair)) {
            return;
        }

        storePair(submittedPair);
        updatePairURL(submittedPair);
    }

    function persistRenderedPair() {
        const pair = getRenderedPair();
        if (!isLikelyAddress(pair)) {
            return;
        }

        const input = getPairInput();
        if (input && !pairValuesMatch(input.value, pair)) {
            input.value = pair;
        }
        storePair(pair);
        updatePairURL(pair);
    }

    function submitPairSearchForm() {
        const form = document.querySelector("[data-pair-search-form]");
        if (!form) {
            return false;
        }

        if (typeof form.requestSubmit === "function") {
            form.requestSubmit();
            return true;
        }

        const button = form.querySelector("[type='submit']");
        if (button && typeof button.click === "function") {
            button.click();
            return true;
        }

        return false;
    }

    function trimTransactions(streamEl) {
        while (streamEl.children.length > maxTransactionRows) {
            streamEl.removeChild(streamEl.lastElementChild);
        }
    }

    function updateTotalTransactionsCount(count) {
        const countEl = document.querySelector("[data-total-transactions-count]");
        const nextCount = Number(count);
        if (!countEl || !Number.isFinite(nextCount) || nextCount <= 0) {
            return;
        }

        const currentCount = Number(String(countEl.textContent || "").replace(/[^\d]/g, ""));
        if (!Number.isFinite(currentCount) || nextCount > currentCount) {
            countEl.textContent = String(nextCount);
        }
    }

    function trimTrailingZeroes(value) {
        return value.replace(/\.?0+$/, "").replace(/\.$/, "") || "0";
    }

    function toSubscriptNumber(value) {
        const digits = ["₀", "₁", "₂", "₃", "₄", "₅", "₆", "₇", "₈", "₉"];
        return String(value)
            .split("")
            .map((char) => digits[Number(char)] || "")
            .join("");
    }

    function formatCompactPrice(value) {
        if (!Number.isFinite(value) || value === 0) {
            return "0.00";
        }

        const sign = value < 0 ? "-" : "";
        const absValue = Math.abs(value);
        if (absValue >= 1) {
            return sign + trimTrailingZeroes(absValue.toFixed(4));
        }
        if (absValue >= 0.0001) {
            return sign + trimTrailingZeroes(absValue.toFixed(8));
        }

        const decimal = trimTrailingZeroes(absValue.toFixed(24));
        if (!decimal.startsWith("0.")) {
            return sign + decimal;
        }

        const fraction = decimal.slice(2);
        let leadingZeroes = 0;
        while (leadingZeroes < fraction.length && fraction[leadingZeroes] === "0") {
            leadingZeroes += 1;
        }
        if (leadingZeroes >= fraction.length) {
            return "0.00";
        }

        let significantDigits = fraction.slice(leadingZeroes, leadingZeroes + 6);
        significantDigits = significantDigits.replace(/0+$/, "") || "0";
        return sign + "0.0" + toSubscriptNumber(leadingZeroes) + significantDigits;
    }

    function precisionForPrice(value) {
        if (!Number.isFinite(value) || value <= 0) {
            return 6;
        }
        if (value >= 1) {
            return 4;
        }
        if (value >= 0.0001) {
            return 8;
        }

        const decimal = value.toFixed(24);
        const fraction = decimal.startsWith("0.") ? decimal.slice(2) : decimal;
        let leadingZeroes = 0;
        while (leadingZeroes < fraction.length && fraction[leadingZeroes] === "0") {
            leadingZeroes += 1;
        }

        return Math.min(Math.max(leadingZeroes + 6, 12), 20);
    }

    function updateSeriesPriceFormat(price) {
        if (!activeCandleSeries || !Number.isFinite(price) || price <= 0) {
            return;
        }

        const precision = precisionForPrice(price);
        if (precision === activePricePrecision) {
            return;
        }

        activePricePrecision = precision;
        activeCandleSeries.applyOptions({
            priceFormat: {
                type: "price",
                precision: precision,
                minMove: 0.0000000000000001,
            },
        });
    }

    function stopChartGapFill() {
        if (!activeChartGapFillTimer) {
            return;
        }

        window.clearInterval(activeChartGapFillTimer);
        activeChartGapFillTimer = null;
    }

    function cancelChartHistoryLoad() {
        if (!activeChartHistoryRequest) {
            return;
        }

        activeChartHistoryRequest.abort();
        activeChartHistoryRequest = null;
    }

    function getLastCandle() {
        let lastCandle = null;
        for (const candle of activeCandles.values()) {
            lastCandle = candle;
        }
        return lastCandle;
    }

    function chartBucketStart(timestamp) {
        if (!Number.isFinite(timestamp) || timestamp <= 0) {
            return 0;
        }

        return Math.floor(timestamp / chartCandleIntervalSeconds) * chartCandleIntervalSeconds;
    }

    function flatCandle(time, price) {
        return {
            time: time,
            open: price,
            high: price,
            low: price,
            close: price,
            volume: 0,
        };
    }

    function applyTradeToCandle(candle, price, volume) {
        candle.high = Math.max(candle.high, price);
        candle.low = Math.min(candle.low, price);
        candle.close = price;
        candle.volume += volume;
    }

    function pruneCandles() {
        let trimmed = false;
        while (activeCandles.size > maxChartCandles) {
            const oldestKey = activeCandles.keys().next().value;
            activeCandles.delete(oldestKey);
            trimmed = true;
        }

        return trimmed;
    }

    function updateLatestCandle(candle) {
        if ((!activeCandleSeries || !activeVolumeSeries || !activeChart) && !activeFallbackChartEl) {
            return;
        }

        activeCandles.set(candle.time, candle);
        const trimmed = pruneCandles();
        updateSeriesPriceFormat(candle.close);
        if (activeFallbackChartEl) {
            renderFallbackChart();
            return;
        }

        if (trimmed) {
            const candleData = Array.from(activeCandles.values());
            activeCandleSeries.setData(candleData);
            activeVolumeSeries.setData(candleData.map(c => ({
                time: c.time,
                value: c.volume || 0,
                color: c.close >= c.open ? "rgba(8, 153, 129, 0.5)" : "rgba(242, 54, 69, 0.5)"
            })));
            return;
        }

        activeCandleSeries.update(candle);
        activeVolumeSeries.update({
            time: candle.time,
            value: candle.volume || 0,
            color: candle.close >= candle.open ? "rgba(8, 153, 129, 0.5)" : "rgba(242, 54, 69, 0.5)"
        });
    }

    function fillFlatCandlesThrough(targetTime) {
        if ((!activeCandleSeries || !activeVolumeSeries || !activeChart) && !activeFallbackChartEl) {
            return;
        }

        let lastCandle = getLastCandle();
        if (!lastCandle) {
            return;
        }

        const targetBucket = chartBucketStart(Number(targetTime));
        if ((targetBucket - lastCandle.time) / chartCandleIntervalSeconds > maxChartCandles) {
            return;
        }

        while (lastCandle.time + chartCandleIntervalSeconds <= targetBucket) {
            const nextCandle = flatCandle(
                lastCandle.time + chartCandleIntervalSeconds,
                lastCandle.close,
            );
            updateLatestCandle(nextCandle);
            lastCandle = nextCandle;
        }
    }

    function startChartGapFill() {
        if (activeChartGapFillTimer) {
            return;
        }

        activeChartGapFillTimer = window.setInterval(function () {
            fillFlatCandlesThrough(Math.floor(Date.now() / 1000));
        }, chartGapFillIntervalMs);
    }

    function setHistoricalCandles(candles) {
        if ((!activeCandleSeries || !activeChart) && !activeFallbackChartEl) {
            return;
        }

        const nextCandles = new Map();
        for (const candle of candles) {
            if (
                !candle ||
                !Number.isFinite(candle.time) ||
                !Number.isFinite(candle.open) ||
                !Number.isFinite(candle.high) ||
                !Number.isFinite(candle.low) ||
                !Number.isFinite(candle.close)
            ) {
                continue;
            }

            nextCandles.set(candle.time, {
                time: candle.time,
                open: candle.open,
                high: candle.high,
                low: candle.low,
                close: candle.close,
                volume: candle.volume || 0,
            });
        }

        if (nextCandles.size === 0) {
            if (activeCandles.size > 0) {
                return;
            }
            if (activeFallbackChartEl) {
                renderFallbackChart();
                return;
            }
            activeCandleSeries.setData([]);
            activeVolumeSeries.setData([]);
            return;
        }

        activeCandles = nextCandles;
        pruneCandles();
        const candleData = Array.from(activeCandles.values());
        if (activeFallbackChartEl) {
            renderFallbackChart();
            const lastFallbackCandle = getLastCandle();
            if (lastFallbackCandle) {
                startChartGapFill();
            }
            return;
        }

        activeCandleSeries.setData(candleData);
        activeVolumeSeries.setData(candleData.map(c => ({
            time: c.time,
            value: c.volume || 0,
            color: c.close >= c.open ? "rgba(8, 153, 129, 0.5)" : "rgba(242, 54, 69, 0.5)"
        })));
        const lastCandle = getLastCandle();
        if (lastCandle) {
            updateSeriesPriceFormat(lastCandle.close);
            startChartGapFill();
        }
        activeChart.timeScale().fitContent();
    }

    function flushPendingChartTicks() {
        if (!activeChartHistoryLoaded || pendingChartTicks.length === 0) {
            return;
        }

        const skipHistoricalTicks = activeCandles.size > 0;
        const queuedTicks = pendingChartTicks
            .slice()
            .sort((left, right) => Number(left.time) - Number(right.time));
        pendingChartTicks = [];
        for (const tick of queuedTicks) {
            if (skipHistoricalTicks && tick.historical) {
                continue;
            }
            upsertChartTick(tick);
        }
    }

    function destroyChart() {
        stopChartGapFill();
        if (activeChartResizeObserver) {
            activeChartResizeObserver.disconnect();
            activeChartResizeObserver = null;
        }
        if (activeChart) {
            activeChart.remove();
            activeChart = null;
        }
        if (activeFallbackChartEl) {
            activeFallbackChartEl.innerHTML = "";
            activeFallbackChartEl = null;
        }

        activeCandleSeries = null;
        activeVolumeSeries = null;
        activeCandles = new Map();
        activeChartHistoryLoaded = false;
        pendingChartTicks = [];
        activePricePrecision = null;
    }

    function createVolumeSeries(chart) {
        const seriesOptions = {
            color: "#26a69a",
            priceFormat: { type: "volume" },
            priceScaleId: "",
        };

        if (typeof chart.addHistogramSeries === "function") {
            return chart.addHistogramSeries(seriesOptions);
        }

        if (
            typeof chart.addSeries === "function" &&
            window.LightweightCharts &&
            window.LightweightCharts.HistogramSeries
        ) {
            return chart.addSeries(
                window.LightweightCharts.HistogramSeries,
                seriesOptions,
            );
        }

        throw new Error("Histogram series API unavailable");
    }

    function createCandlestickSeries(chart) {
        const seriesOptions = {
            upColor: "#089981",
            downColor: "#f23645",
            borderVisible: true,
            wickVisible: true,
            borderColor: "#089981",
            wickColor: "#089981",
            borderUpColor: "#089981",
            borderDownColor: "#f23645",
            wickUpColor: "#089981",
            wickDownColor: "#f23645",
            priceLineVisible: true,
            lastValueVisible: true,
        };

        if (typeof chart.addCandlestickSeries === "function") {
            return chart.addCandlestickSeries(seriesOptions);
        }

        if (
            typeof chart.addSeries === "function" &&
            window.LightweightCharts &&
            window.LightweightCharts.CandlestickSeries
        ) {
            return chart.addSeries(
                window.LightweightCharts.CandlestickSeries,
                seriesOptions,
            );
        }

        throw new Error("Candlestick series API unavailable");
    }

    function fallbackY(price, minPrice, maxPrice, top, height) {
        if (maxPrice <= minPrice) {
            return top + height / 2;
        }

        return top + ((maxPrice - price) / (maxPrice - minPrice)) * height;
    }

    function renderFallbackChart() {
        if (!activeFallbackChartEl) {
            return;
        }

        const candles = Array.from(activeCandles.values());
        if (candles.length === 0) {
            activeFallbackChartEl.innerHTML = "";
            return;
        }

        const width = Math.max(activeFallbackChartEl.clientWidth || 960, 320);
        const height = Math.max(activeFallbackChartEl.clientHeight || 420, 260);
        const marginTop = 18;
        const marginRight = 76;
        const marginBottom = 24;
        const marginLeft = 12;
        const volumeHeight = 66;
        const plotWidth = width - marginLeft - marginRight;
        const priceHeight = height - marginTop - marginBottom - volumeHeight - 14;
        const volumeTop = marginTop + priceHeight + 14;

        let minPrice = candles[0].low;
        let maxPrice = candles[0].high;
        let maxVolume = 0;
        for (const candle of candles) {
            minPrice = Math.min(minPrice, candle.low);
            maxPrice = Math.max(maxPrice, candle.high);
            maxVolume = Math.max(maxVolume, candle.volume || 0);
        }

        const padding = (maxPrice - minPrice) * 0.08 || maxPrice * 0.08 || 1;
        minPrice = Math.max(minPrice - padding, 0);
        maxPrice = maxPrice + padding;

        const slot = plotWidth / Math.max(candles.length, 1);
        const bodyWidth = Math.max(Math.min(slot * 0.62, 10), 3);
        const parts = [
            `<svg viewBox="0 0 ${width} ${height}" width="100%" height="100%" role="img" aria-label="Candlestick chart">`,
            `<rect x="0" y="0" width="${width}" height="${height}" fill="transparent"/>`,
        ];

        for (let i = 0; i <= 4; i += 1) {
            const y = marginTop + (priceHeight / 4) * i;
            parts.push(`<line x1="${marginLeft}" y1="${y}" x2="${width - marginRight}" y2="${y}" stroke="rgba(96,165,250,0.16)" stroke-width="1"/>`);
        }

        candles.forEach((candle, index) => {
            const x = marginLeft + index * slot + slot / 2;
            const openY = fallbackY(candle.open, minPrice, maxPrice, marginTop, priceHeight);
            const closeY = fallbackY(candle.close, minPrice, maxPrice, marginTop, priceHeight);
            const highY = fallbackY(candle.high, minPrice, maxPrice, marginTop, priceHeight);
            const lowY = fallbackY(candle.low, minPrice, maxPrice, marginTop, priceHeight);
            const up = candle.close >= candle.open;
            const color = up ? "#22c55e" : "#ef4444";
            const bodyY = Math.min(openY, closeY);
            const bodyHeight = Math.max(Math.abs(closeY - openY), 1.5);
            const volume = candle.volume || 0;
            const volumeBarHeight = maxVolume > 0 ? (volume / maxVolume) * (volumeHeight - 8) : 0;

            parts.push(`<line x1="${x}" y1="${highY}" x2="${x}" y2="${lowY}" stroke="${color}" stroke-width="1.4"/>`);
            parts.push(`<rect x="${x - bodyWidth / 2}" y="${bodyY}" width="${bodyWidth}" height="${bodyHeight}" fill="${color}" rx="1"/>`);
            if (volumeBarHeight > 0) {
                parts.push(`<rect x="${x - bodyWidth / 2}" y="${volumeTop + volumeHeight - volumeBarHeight}" width="${bodyWidth}" height="${volumeBarHeight}" fill="${up ? "rgba(34,197,94,0.45)" : "rgba(239,68,68,0.45)"}"/>`);
            }
        });

        parts.push(`<text x="${width - marginRight + 10}" y="${marginTop + 10}" fill="rgba(255,255,255,0.82)" font-size="12" font-family="ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace">${formatCompactPrice(maxPrice)}</text>`);
        parts.push(`<text x="${width - marginRight + 10}" y="${marginTop + priceHeight}" fill="rgba(255,255,255,0.82)" font-size="12" font-family="ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace">${formatCompactPrice(minPrice)}</text>`);
        parts.push(`<text x="${marginLeft}" y="${height - 6}" fill="rgba(191,219,254,0.82)" font-size="12" font-family="ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace">${candles.length} candles</text>`);
        parts.push("</svg>");

        activeFallbackChartEl.innerHTML = parts.join("");
    }

    function enableFallbackChart(chartEl, reason) {
        if (activeChart) {
            activeChart.remove();
            activeChart = null;
        }

        activeCandleSeries = null;
        activeVolumeSeries = null;
        activeFallbackChartEl = chartEl;
        activeFallbackChartEl.innerHTML = "";
        console.warn("[chart] using fallback renderer", reason || "");

        activeChartResizeObserver = new ResizeObserver(function () {
            renderFallbackChart();
        });
        activeChartResizeObserver.observe(chartEl);
    }

    function initializeChart() {
        const chartEl = document.getElementById("chartView");
        destroyChart();

        if (!chartEl) {
            return;
        }
        if (useNativeChartRenderer) {
            enableFallbackChart(chartEl, "native renderer");
            updateChartStatus("Loading past swaps...", "info");
            return;
        }
        if (!window.LightweightCharts) {
            enableFallbackChart(chartEl, "library unavailable");
            updateChartStatus("Loading past swaps...", "info");
            return;
        }

        activeChart = window.LightweightCharts.createChart(chartEl, {
            width: chartEl.clientWidth || 960,
            height: chartEl.clientHeight || 420,
            layout: {
                background: { color: "transparent" },
                textColor: "rgba(255, 255, 255, 0.86)",
                fontFamily: "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace",
                attributionLogo: false,
            },
            localization: {
                priceFormatter: formatCompactPrice,
                timeFormatter: (t) => {
                    const d = new Date(t * 1000);
                    return d.getHours().toString().padStart(2, '0') + ':' + d.getMinutes().toString().padStart(2, '0');
                }
            },
            grid: {
                vertLines: { color: "rgba(59, 130, 246, 0.12)" },
                horzLines: { color: "rgba(59, 130, 246, 0.16)" },
            },
            rightPriceScale: {
                borderColor: "rgba(59, 130, 246, 0.45)",
                autoScale: true,
                alignLabels: true,
                mode: 1,
                scaleMargins: {
                    top: 0.1,
                    bottom: 0.2,
                },
            },
            timeScale: {
                borderColor: "rgba(59, 130, 246, 0.26)",
                timeVisible: true,
                secondsVisible: false,
                rightOffset: 1,
                barSpacing: 20,
                minBarSpacing: 10,
                fixLeftEdge: true,
                lockVisibleTimeRangeOnResize: true,
                shiftVisibleRangeOnNewBar: false,
            },
            crosshair: {
                vertLine: { color: "rgba(96, 165, 250, 0.35)" },
                horzLine: { color: "rgba(96, 165, 250, 0.35)" },
            },
        });

        try {
            activeCandleSeries = createCandlestickSeries(activeChart);
            activeVolumeSeries = createVolumeSeries(activeChart);
            activeVolumeSeries.priceScale().applyOptions({
                scaleMargins: {
                    top: 0.8,
                    bottom: 0,
                },
            });
        } catch (error) {
            console.error("[chart] series init failed", error);
            enableFallbackChart(chartEl, error);
            updateChartStatus("Loading past swaps...", "info");
            return;
        }

        activeChartResizeObserver = new ResizeObserver((entries) => {
            for (const entry of entries) {
                if (!activeChart) {
                    return;
                }

                activeChart.applyOptions({
                    width: entry.contentRect.width,
                    height: entry.contentRect.height,
                });
            }
        });
        activeChartResizeObserver.observe(chartEl);

        updateChartStatus("Loading past swaps...", "info");
    }

    function rebuildChartData() {
        if ((!activeCandleSeries || !activeChart) && !activeFallbackChartEl) {
            return;
        }

        if (activeFallbackChartEl) {
            renderFallbackChart();
            return;
        }

        activeCandleSeries.setData(Array.from(activeCandles.values()));
        activeChart.timeScale().fitContent();
    }

    function upsertChartTick(tick) {
        if (!activeCandleSeries && !activeFallbackChartEl) {
            return;
        }

        const price = Number(tick.price);
        const timestamp = Number(tick.time);
        if (!Number.isFinite(price) || !Number.isFinite(timestamp) || price <= 0) {
            return;
        }

        const rawVolume = Number(tick.volume);
        const volume = Number.isFinite(rawVolume) && rawVolume > 0 ? rawVolume : 0;
        const bucketTime = chartBucketStart(timestamp);
        let candle = activeCandles.get(bucketTime);
        if (candle) {
            applyTradeToCandle(candle, price, volume);
            updateLatestCandle(candle);
        } else {
            let lastCandle = getLastCandle();
            if (lastCandle && bucketTime < lastCandle.time) {
                return;
            }

            if (lastCandle) {
                fillFlatCandlesThrough(bucketTime - chartCandleIntervalSeconds);
                lastCandle = getLastCandle();
            }

            const open = lastCandle ? lastCandle.close : price;
            candle = {
                time: bucketTime,
                open: open,
                high: Math.max(open, price),
                low: Math.min(open, price),
                close: price,
                volume: volume,
            };
            updateLatestCandle(candle);
        }

        if (activeCandles.size > 0) {
            startChartGapFill();
        }

        if (activeCandles.size === 1 && activeChart) {
            activeChart.timeScale().fitContent();
        }

        const typeLabel = tick.type ? tick.type + " " : "";
        updateChartStatus(typeLabel + "tick " + (tick.priceDisplay || formatCompactPrice(price)) + " WETH", "success");
    }

    function loadChartHistory() {
        const chartEl = document.getElementById("chartView");
        if (!chartEl) {
            return;
        }

        const chartURL = chartEl.getAttribute("data-chart-url") || "";
        if (!chartURL) {
            activeChartHistoryLoaded = true;
            updateChartStatus("Chart unavailable for this result.", "error");
            return;
        }

        cancelChartHistoryLoad();
        const controller = new AbortController();
        activeChartHistoryRequest = controller;
        activeChartHistoryLoaded = false;
        pendingChartTicks = [];
        updateChartStatus("Loading past swaps...", "info");

        fetch(chartURL, {
            method: "GET",
            headers: {
                Accept: "application/json",
            },
            signal: controller.signal,
        })
            .then(function (response) {
                if (!response.ok) {
                    throw new Error("chart history request failed with status " + response.status);
                }
                return response.json();
            })
            .then(function (candles) {
                if (controller.signal.aborted) {
                    return;
                }

                if (Array.isArray(candles)) {
                    setHistoricalCandles(candles);
                }

                activeChartHistoryLoaded = true;
                flushPendingChartTicks();

                if (activeCandles.size === 0) {
                    updateChartStatus("Waiting for first swap to build candles.", "info");
                    return;
                }

                updateChartStatus("Showing loaded candles.", "success");
            })
            .catch(function (error) {
                if (error.name === "AbortError") {
                    return;
                }

                console.error("[chart] history error", error);
                activeChartHistoryLoaded = true;
                flushPendingChartTicks();
                updateChartStatus("Recent candle load failed. Live chart is still running.", "error");
            })
            .finally(function () {
                if (activeChartHistoryRequest === controller) {
                    activeChartHistoryRequest = null;
                }
            });
    }

    function connectTransactionStream() {
        const streamEl = document.getElementById("transactionsStream");
        if (!streamEl) {
            closeStream("transactions container missing");
            destroyChart();
            return;
        }

        const streamURL = streamEl.getAttribute("data-stream-url") || "";
        const resolvedPair = streamEl.getAttribute("data-resolved-pair") || "";
        if (!streamURL || !resolvedPair) {
            updateStreamStatus("Live transaction stream unavailable for this result.", true);
            updateChartStatus("Chart unavailable for this result.", "error");
            closeStream("stream metadata missing");
            return;
        }

        if (activeEventSource && activeStreamURL === streamURL) {
            return;
        }

        closeStream("switching streams");
        updateStreamStatus("Connecting to live transaction stream...", false);
        if (activeChartHistoryLoaded) {
            updateChartStatus("Connecting chart to live swaps...", "info");
        }
        console.info("[sse] opening", streamURL);

        const eventSource = new EventSource(streamURL);
        activeEventSource = eventSource;
        activeStreamURL = streamURL;

        eventSource.onopen = function () {
            console.info("[sse] open", streamURL);
            activeStreamReconnectDelayMs = streamReconnectMinDelayMs;
            updateStreamStatus("Live transaction stream connected.", false);
            if (activeChartHistoryLoaded && activeCandles.size === 0) {
                updateChartStatus("Waiting for first swap to build candles.", "info");
            }
        };

        eventSource.addEventListener("message", function (event) {
            console.info("[sse] message", streamURL);
            streamEl.insertAdjacentHTML("afterbegin", event.data);
            trimTransactions(streamEl);
            updateStreamStatus("Live transaction stream connected.", false);
        });

        eventSource.addEventListener("candles", function (event) {
            try {
                const candles = JSON.parse(event.data);
                if (!Array.isArray(candles)) {
                    return;
                }

                setHistoricalCandles(candles);
                activeChartHistoryLoaded = true;
                flushPendingChartTicks();
                if (activeCandles.size > 0) {
                    updateChartStatus("Showing loaded candles.", "success");
                }
            } catch (error) {
                console.error("[chart] invalid candle snapshot", error, event.data);
                updateChartStatus("Candle snapshot decode failed.", "error");
            }
        });

        eventSource.addEventListener("stats", function (event) {
            try {
                const stats = JSON.parse(event.data);
                updateTotalTransactionsCount(stats && stats.transactionCount);
            } catch (error) {
                console.error("[sse] invalid stats", error, event.data);
            }
        });

        eventSource.addEventListener("chart", function (event) {
            try {
                const tick = JSON.parse(event.data);
                if (!activeChartHistoryLoaded) {
                    pendingChartTicks.push(tick);
                    if (pendingChartTicks.length > maxChartCandles) {
                        pendingChartTicks.shift();
                    }
                    return;
                }

                if (tick.historical && activeCandles.size > 0) {
                    return;
                }

                upsertChartTick(tick);
            } catch (error) {
                console.error("[chart] invalid tick", error, event.data);
                updateChartStatus("Chart tick decode failed.", "error");
            }
        });

        eventSource.onerror = function (event) {
            console.error("[sse] error", streamURL, event);
            if (activeEventSource !== eventSource) {
                return;
            }

            eventSource.close();
            activeEventSource = null;
            activeStreamURL = "";
            updateStreamStatus("Live transaction stream reconnecting...", true);
            updateChartStatus("Chart stream reconnecting...", "error");
            scheduleStreamReconnect();
        };
    }

    document.addEventListener("DOMContentLoaded", function () {
        const restoredPair = restorePairInput();
        const renderedPair = getRenderedPair();
        if (
            isLikelyAddress(restoredPair) &&
            (!isLikelyAddress(renderedPair) || !pairValuesMatch(renderedPair, restoredPair))
        ) {
            storePair(restoredPair);
            updatePairURL(restoredPair);
            submitPairSearchForm();
            return;
        }

        persistRenderedPair();
        initializeChart();
        loadChartHistory();
        connectTransactionStream();
    });

    document.addEventListener("submit", function (event) {
        const form = event.target;
        if (form && form.matches && form.matches("[data-pair-search-form]")) {
            persistSubmittedPair();
        }
    });

    document.body.addEventListener("htmx:beforeRequest", function (event) {
        const target = event.detail && event.detail.target;
        if (target && target.id === "result") {
            persistSubmittedPair();
            cancelChartHistoryLoad();
            closeStream("metadata refresh");
            destroyChart();
        }
    });

    document.body.addEventListener("htmx:afterSwap", function (event) {
        const target = event.detail && event.detail.target;
        if (target && target.id === "result") {
            persistRenderedPair();
            initializeChart();
            loadChartHistory();
            connectTransactionStream();
        }
    });

    window.addEventListener("beforeunload", function () {
        cancelChartHistoryLoad();
        closeStream("page unload");
        destroyChart();
    });
})();
