var QosStatistics = {};
QosStatistics.Plot = {};

function QosStatistics.Plot.Base(constraint, data) {
  this.addDataToPlot = function(rows) {
  }

  this.addToSeries = function(series, row) { /* override this */ }

  this.getLastTimestamp = function() {
    return this.lastTimestamp;
  }

  this.getRuntime = function(time) {
    return time - this.startTimestamp;
  }
};

QosStatistics.Plot.EmitConsume = function(constraint, data) {
  this.totalEmitRate = function(row) { return [row.ts, row.totalEmitRate]; };
  this.totalConsumeRate = function(row) { return [row.ts, row.totalConsumeRate]; };
  this.emitConsumeRatio = function(row) { return [row.ts, row.totalEmitRate / row.totalConsumeRate]; };

  this.prepareSeries = function(rows) {
    return [
      { label: 'Total emitter rate', lines: { show: true }, data: rows.map(this.totalEmitRate) },
      { label: 'Total consumer rate', lines: { show: true }, data: rows.map(this.totalConsumeRate) },
      { label: 'Emit/consume', lines: { show: true }, yaxis: 2, data: rows.map(this.emitConsumeRatio) }
    ];
  };

  this.addToSeries = function(series, row) {
    series[0].data.push([row.ts, row.totalEmitRate]);
    series[1].data.push([row.ts, row.totalConsumeRate]);
    series[2].data.push([row.ts, item.runtimeStart, row.totalEmitRate / row.totalConsumeRate]);
  };
};
$.extend(QosStatistics.Plot.EmitConsume, QosStatistics.Plot.Base);
