/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

$(startJobPolling);

var jobs = { };
var lastTimestamp = null;
var MIN_ZOOM_ELEMENTS = 5;
var MIN_DATA_POINTS_COUNT = 2;
// colors are subset of d3.scale.category10 (https://github.com/mbostock/d3/wiki/Ordinal-Scales#category10)
var chartColors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#e377c2', '#7f7f7f', '#bcbd22'];
var plotOptions = {
  series: { shadowSize: 0 }, // Drawing is faster without shadows
  colors: chartColors,
  points: { show: true, radius: 1.3 },
  legend: { position: 'nw' },
  yaxis: { zoomRange: false, panRange: false, min: 0 },
  zoom: { interactive: true },
  pan: { interactive: true },
  grid: { hoverable: true, autoHighlight: true }
};

function initJobPanel(job, name, startTime) {
  var box = $('<div id="job-' + job + '" class="panel panel-info"/>');
      box.append('<div class="panel-heading" title="' + job + '">' + '<h3 class="panel-title">'
          + name
          + '<span class="badge pull-right">' + startTime + '</span></h3></div>');
      box.append('<div class="list-group" />');
  return box;
}

function showWaitingForDataBanner(job, currentPointCount, requiredCount) {
  var box = $('<div class="list-group-item" id="waiting-for-data-banner">'
      + '<i class="fa fa-spinner fa-spin"></i>'
      + ' Waiting for data... (got ' + currentPointCount + ' of at least ' + requiredCount + ' data points)'
      + '</div>');
  $('#waiting-for-data-banner').remove();
  $('#job-' + job + ' .list-group').append(box);
}

function addConstraintGroup(job, constraint, name) {
  var name = name.replace(/->/, '<i class="fa fa-long-arrow-right"></i>');
  var box = $('<div id="constraint-' + constraint + '" class="constraint"/>');
      box.append('<div class="list-group-item constraint-title"><h3 class="panel-title">' + name + '</h3></div>');
      box.find('.panel-title').append('<i class="fa fa-spinner fa-spin pull-right chart-loading-icon"></i>');

  $('#job-' + job + ' .list-group').append(box);
}

function addChartGroup(constraint, chart, title, leftAxisLabel, rightAxisLabel) {
  var title = $('<div class="list-group-item">' + title + '</div>');
      title.append('<i class="fa fa-spinner fa-spin pull-right chart-loading-icon"></i>');
  $('#constraint-' + constraint).append(title);

  var box = $('<div class="list-group-item qos-chart-box" />');
  if (leftAxisLabel)
    box.append('<div class="axis-label left-axis-label">' + leftAxisLabel + '</div>');
  if (rightAxisLabel)
    box.append('<div class="axis-label right-axis-label">' + rightAxisLabel + '</div>');
  box.append('<div class="chart" data-chart="' + chart + '"/>');
  box.append('<div class="axis-label bottom-axis-label"><span>Runtime [s]</span></div>');

  $('#constraint-' + constraint).append(box);
}

function updateJobOverviewList(jobId, jobJson, currentJob) {
  if ($('#job-overview-dropdown li[data-job="' + jobId + '"]').length == 0) {
    $('#job-overview-dropdown').append(
        '<li data-job="' + jobId + '"><a href="?job=' + jobId + '">'
            + jobJson.name + ' (' + formattedTimeFromTimestamp(jobJson.creationTimestamp) + ')'
            + '</a></li>');
  }

  if (jobId == currentJob)
    $('#job-overview-dropdown li[data-job="' + jobId + '"]').addClass('active');
}

/** Convert raw latency json data into flot series. */
function prepareLatencySeries(latency) {
    var series = [];
    var edgeLabelAdded = false;
    var edgeOblLabelAdded = false;

    $.each(latency.types, function(col, type) {
      var s = {
        data: latency.rows.map(function(row) { return [row.ts, row.values[col]]; }),
        stack: true
      };

      // color
      if (type == "edge") { s.color = "lightgrey"; }
      else if (type == "edgeObl") { s.color = "darkgrey"; }
      if (col > 0) { s.fillBetween = col - 1; /* last series */ }

      // labels
      if (type == "vertex") {
        s.label = latency.labels[col];
      } else if (type == "edge" && !edgeLabelAdded) {
        s.label = "Transport";
        edgeLabelAdded = true;
      } else if (type == "edgeObl" && !edgeOblLabelAdded) {
        s.label = "Output Buffer";
        edgeOblLabelAdded = true
      } else if (type == "edge") {
        s.tooltip = "Transport";
      } else if (type == "edgeObl") {
        s.tooltip = "Output Buffer";
      }

      series.push(s);
    });

    // add min/max after normal series (otherwise change fillBetween numbers!)
    var minRawData = latency.rows.map(function(row) { return [row.ts, row.min]; });
    var maxRawData = latency.rows.map(function(row) { return [row.ts, row.max]; });
    series.push({ label: 'Min / Max', tooltip: 'Min', color: 'black', lines: { fill: false }, data: minRawData });
    series.push({ color: 'black', tooltip: 'Max', lines: { fill: false }, data: maxRawData });

    return series;
}

/** Prepares one emit/consume plot */
function prepareEmitConsumeSeries(rows) {
  var series = [];

  var emitRate = rows.map(function(row) { return [row.ts, row.totalEmitRate]; });
  series.push({ label: 'Total emitter rate', lines: { show: true }, data: emitRate });
  var consumeRate = rows.map(function(row) { return [row.ts, row.totalConsumeRate]; });
  series.push({ label: 'Total consumer rate', lines: { show: true }, data: consumeRate });
  var emitConsume = rows.map(function(row) { return [row.ts, row.totalEmitRate / row.totalConsumeRate]; });
  series.push({ label: 'Emit/consume ratio', lines: { show: true }, yaxis: 2, data: emitConsume });

  return series;
}

function prepareCpuLoadSeries(index, rows) {
  return series = [
    { label: 'Unknown', stack: true, data: rows.map(function(row) { return [row.ts, row.values[index][0]]; }) },
    { label: 'Low', stack: true, data: rows.map(function(row) { return [row.ts, row.values[index][1]]; }) },
    { label: 'Medium', stack: true, data: rows.map(function(row) { return [row.ts, row.values[index][2]]; }) },
    { label: 'High', stack: true, data: rows.map(function(row) { return [row.ts, row.values[index][3]]; }) }
  ];
}

function formatTimeSince(time) {
  return (time / 1000).toFixed(0) + 's';
}


function addPlotHoverHandler(plot, startTimestamp) {
  var tooltipInner = '';
  var lastDataIndex = -1;
  var lastSeriesIndex = -1;
  var timeFormatter = getRuntimeTickFormatter(startTimestamp);

  plot.getPlaceholder().bind('plothover', function(event, pos, item) {
    if (item) {
      var tooltip = $('#chart-tooltip');

      if (lastDataIndex != item.dataIndex) {
        var tooltipInner = $('<table class="table" />');
            tooltipInner.append('<tr><td class="legendLabel">Runtime:</dt><td>' + timeFormatter(item.datapoint[0]) + 's</dt></tr>');

        $.each(plot.getData(), function(i, series) {
          var label = series.tooltip ? series.tooltip : series.label;
          var value = series.data[item.dataIndex][1].toFixed(2);
          var suffix = series.tooltipSuffix ? series.tooltipSuffix : '';
            tooltipInner.append('<tr><td>' + label + ':</dt><td>' + value + suffix + '</dt></tr>');
        });
        tooltip.html(tooltipInner);
      }

      if (lastDataIndex != item.dataIndex || lastSeriesIndex != item.seriesIndex) {
        var windowWidth = $(window).width();
        var tooltipWidth = tooltip.width();

        if (item.pageX + tooltipWidth + 30 < windowWidth) {
          tooltip.css({ top: item.pageY, left: item.pageX + 10, right: '' });
        } else {
          tooltip.css({ top: item.pageY, left: '', right: windowWidth - item.pageX - 10 });
        }
      }

      lastDataIndex = item.dataIndex;
      lastSeriesIndex = item.seriesIndex;

      $('#chart-tooltip').fadeIn(200);
    } else {
      $('#chart-tooltip').hide();
    }
  });
}

function getRuntimeTickFormatter(startTimestamp) {
  return function(time) {
    return ((time - startTimestamp) / 1000).toFixed(0);
  };
}

/** Add DOM elements and create flot plots. */
function setupPlots(jobId, json, refreshInterval) {
  console.debug("[" + (new Date()) +"] initializing plots.");
  var statistics = { };
  var startTimestamp = json.creationTimestamp;
  var minZoom = 5 * refreshInterval;

  for (id in json.constraints) {
    var constraint = json.constraints[id];

    // initialize constraint and plot boxes
    addConstraintGroup(jobId, id, constraint.name);
    addChartGroup(id, 'latencies', 'Latencies', 'Latency [ms]');
    $.each(constraint.emitConsume, function(i, data) {
      addChartGroup(id, 'emit-consume-edge-' + i, 'Emit and consume on edge ' + i, 'Records [1/s]', 'Ratio');
    });
    $.each(constraint.cpuLoads.header, function(i, vertex) {
      addChartGroup(id, 'cpu-load-vertex-' + i, 'CPU load on vertex ' + vertex.name, 'Instances');
    });

    // initilize the charts (after adding dom elements)
    statistics[id] = {};

    // latencies plot
    var series = prepareLatencySeries(constraint.latencies);
    var minTimestamp = getFirstTimestamp(constraint.latencies.rows);
    var maxTimestamp = getLastTimestamp(constraint.latencies.rows);
    statistics[id].latencies = {
      plot: $.plot('#constraint-' + id + ' [data-chart="latencies"]', series, $.extend(true, {
          series: { tooltipSuffix: 'ms' },
          lines: { show: true, fill: 0.8 },
          minZoom: minZoom,
          xaxis: { zoomRange: false, panRange: false,
                   tickFormatter: getRuntimeTickFormatter(startTimestamp) },
        }, plotOptions)
      ),
      lastTimestamp: maxTimestamp
    };
    statistics[id].latencies.plot.recalculatePanAndZoomRange();
    addPlotHoverHandler(statistics[id].latencies.plot, startTimestamp);

    // emit/consume plots
    statistics[id].emitConsume = [];
    $.each(constraint.emitConsume, function(i, data) {
      var series = prepareEmitConsumeSeries(data);
      var minTimestamp = getFirstTimestamp(data);
      var maxTimestamp = getLastTimestamp(data);

      statistics[id].emitConsume.push({
        plot: $.plot('#constraint-' + id + ' [data-chart="emit-consume-edge-' + i + '"]', series, $.extend(true, {
            minZoom: minZoom,
            xaxis: { zoomRange: false, panRange: false,
                     tickFormatter: getRuntimeTickFormatter(startTimestamp) },
            yaxes: [ {}, { position: 'right', alignTicksWithAxis: 1 } ],
          }, plotOptions)
        ),
        lastTimestamp: maxTimestamp
      });
      statistics[id].emitConsume[i].plot.recalculatePanAndZoomRange();
      addPlotHoverHandler(statistics[id].emitConsume[i].plot, startTimestamp);
    });

    // cpu load plots
    statistics[id].cpuLoads = [];
    $.each(constraint.cpuLoads.header, function(index, vertex) {
      var series = prepareCpuLoadSeries(index, constraint.cpuLoads.values);
      var minTimestamp = getFirstTimestamp(constraint.cpuLoads.values);
      var maxTimestamp = getLastTimestamp(constraint.cpuLoads.values);

      statistics[id].cpuLoads.push({
        plot: $.plot('#constraint-' + id + ' [data-chart="cpu-load-vertex-' + index + '"]', series, $.extend(true, {
          lines: { show: true, fill: 0.8, lineWidth: 0 },
            minZoom: minZoom,
            xaxis: { zoomRange: false, panRange: false,
                     tickFormatter: getRuntimeTickFormatter(startTimestamp) },
          }, plotOptions)
        ),
        lastTimestamp: maxTimestamp
      });
      statistics[id].cpuLoads[index].plot.recalculatePanAndZoomRange();
      addPlotHoverHandler(statistics[id].cpuLoads[index].plot, startTimestamp);
    });
  }

  console.debug("[" + (new Date()) +"] initializing plots done.");

  return statistics;
}

function addLatencyData(latencies, newLatencyJson, maxEntriesCount) {
  return addDataToPlot(latencies, newLatencyJson, maxEntriesCount, function(newData, row) {
    $.each(row.values, function(col, value) {
      newData[col].data.push([row.ts, value]);
    });
    newData[newData.length - 2].data.push([row.ts, row.min]);
    newData[newData.length - 1].data.push([row.ts, row.max]);
  });
}

function addEmitConsumeData(item, rows, maxEntriesCount) {
  return addDataToPlot(item, rows, maxEntriesCount, function(newData, row) {
    newData[0].data.push([row.ts, row.totalEmitRate]);
    newData[1].data.push([row.ts, row.totalConsumeRate]);
    newData[2].data.push([row.ts, row.totalEmitRate / row.totalConsumeRate]);
  });
}

function addCpuLoadData(item, index, rows, maxEntriesCount) {
  return addDataToPlot(item, rows, maxEntriesCount, function(newData, row) {
    $.each(row.values[index], function(i, value) {
      newData[i].data.push([row.ts, value]);
    });
  });
}

function addDataToPlot(item, rows, maxEntriesCount, addRowCallback) {
  var newData = item.plot.getData();
  var isZoomed = item.plot.isZoomed();

  // add new elements
  $.each(rows, function(i, row) {
    if (row.ts > item.lastTimestamp) {
      addRowCallback(newData, row);
      item.lastTimestamp = row.ts;
    }
  });

  // remove old elements
  $.each(newData, function(i, series) {
    if(series.data.length > maxEntriesCount)
        series.data = series.data.slice(series.data.length - maxEntriesCount);
  });

  item.plot.setData(newData);
  item.plot.recalculatePanAndZoomRange();

  if (isZoomed)
    // no need to update grid: (latencies.plot.setupGrid())
    item.plot.draw();
  else
    item.plot.zoomOut(); // this calls setupGrid+draw too
}

function updatePlots(statistics, json, maxEntriesCount) {
  console.debug("[" + (new Date()) +"] updating stats.");

  for(id in json.constraints) {
    addLatencyData(statistics[id].latencies, json.constraints[id].latencies.rows, maxEntriesCount);

    $.each(json.constraints[id].emitConsume, function(i, data) {
      addEmitConsumeData(statistics[id].emitConsume[i], data, maxEntriesCount);
    });

    $.each(json.constraints[id].cpuLoads.header, function(i, vertex) {
        addCpuLoadData(statistics[id].cpuLoads[i], i, json.constraints[id].cpuLoads.values, maxEntriesCount);
    });
  }

  console.debug("[" + (new Date()) +"] updating stats done.");
}

function getFirstTimestamp(data) {
  return  (data.length > 0) ? data[0].ts : Infinity;
}

function getLastTimestamp(data) {
  return (data.length > 0) ? data[data.length - 1].ts : Infinity;
}

function getNextStartTimestamp(statistics) {
  var nextTimestamp = Infinity;

  for(id in statistics) {
    nextTimestamp = Math.min(nextTimestamp, statistics[id].latencies.lastTimestamp);
    $.each(statistics[id].emitConsume, function(i, data) {
      nextTimestamp = Math.min(nextTimestamp, data.lastTimestamp);
    });
    $.each(statistics[id].cpuLoads, function(i, data) {
      nextTimestamp = Math.min(nextTimestamp, data.lastTimestamp);
    });
  }

  return nextTimestamp;
}

/** Returns smallest point count from all data sources (plots). */
function getMinDataPointCount(jobJson) {
    var result = Infinity;

    for (id in jobJson.constraints) {
      var constraint = jobJson.constraints[id];

      result = Math.min(result, constraint.latencies.rows.length);
      $.each(constraint.emitConsume, function(i, rows) {
        result = Math.min(result, rows.length);
      });
      result = Math.min(result, constraint.cpuLoads.values.length);
    }

    if (result == Infinity)
      result = 0;

    return result;
}

function updateJob(jobId, jobJson, refreshInterval, maxEntriesCount) {
  if (jobs[jobId] == null) {
    var minDataPointCount = getMinDataPointCount(jobJson);

    if ($('#job-' + jobId).length == 0) {
      $('#jobs').append(initJobPanel(jobId, jobJson.name,
            formattedTimeFromTimestamp(jobJson.creationTimestamp)));
    }

    if (minDataPointCount < MIN_DATA_POINTS_COUNT)
      showWaitingForDataBanner(jobId, minDataPointCount, MIN_DATA_POINTS_COUNT);

    if (minDataPointCount >= MIN_DATA_POINTS_COUNT) {
      jobs[jobId] = setupPlots(jobId, jobJson, refreshInterval);
      $('#waiting-for-data-banner').hide();
    }

  } else {
    updatePlots(jobs[jobId], jobJson, maxEntriesCount);
  }

  return getNextStartTimestamp(jobs[jobId]);
}

function pollStatistics(currentJob, interval, lastTimestamp) {
  $('.chart-loading-icon').fadeIn();

  $.getJSON('/qos-statistics', { job: currentJob, startTimestamp: lastTimestamp })
    .done(function(json) {
      var nextTimestamp = Infinity;

      if (!$.isEmptyObject(json.jobs)) {
        if (currentJob == null)
          currentJob = json.lastStartedJob;

        for(var jobId in json.jobs) {
          updateJobOverviewList(jobId, json.jobs[jobId], currentJob);
          nextTimestamp = Math.min(nextTimestamp, updateJob(jobId, json.jobs[jobId], json.refreshInterval, json.maxEntriesCount));
        }

        $('.chart-loading-icon').fadeOut();
        $('#loading-banner').hide();
        $('#job-overview-dropdown').parent('.btn-group').removeClass('hidden');
      }

      if (nextTimestamp == Infinity)
        nextTimestamp = null;
      else
        nextTimestamp += 1;

      setTimeout(pollStatistics, json.refreshInterval, currentJob, json.refreshInterval, nextTimestamp)
    })
    .error(function() {
      setTimeout(pollStatistics, interval, jobId, interval);
    })
}

function startJobPolling() {
  var currentJob = null;

  if (location.search && location.search.length > 1 && /^\?job=/.test(location.search))
    currentJob = location.search.replace(/^\?job=(\w+)$/, "$1");

  pollStatistics(currentJob, 1000, null);
}
