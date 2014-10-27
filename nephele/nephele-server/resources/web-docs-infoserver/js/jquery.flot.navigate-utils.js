
(function ($) {
    function init(plot) {
      /** Calculates pan range and zoom option. */
      plot.recalculatePanAndZoomRange = function() {
        var minZoom = plot.getOptions().minZoom;
        var minValue = Infinity;
        var maxValue = 0;

        $.each(plot.getData(), function(i, series) {
          if (series.data.length > 0) {
            minValue = Math.min(minValue, series.data[0][0]);
            maxValue = Math.max(maxValue, series.data[series.data.length - 1][0]);
          }
        });

        var rangeDiff = maxValue - minValue;
        $.each(plot.getXAxes(), function(i, axis) {
          axis.options.panRange = [minValue, maxValue];
          axis.options.zoomRange = (rangeDiff >= minZoom) ? [minZoom, rangeDiff] : false;
        });
      }

      /** Validates if x axis min/max equals pan range. */
      plot.isZoomed = function() {
        return plot.getXAxes().reduce(function(previousValue, xAxis, index, array) {
          if (xAxis.options.panRange)
            return previousValue || xAxis.min != xAxis.options.panRange[0] || xAxis.max != xAxis.options.panRange[1];
          else
            return previousValue;
        }, false);
      }

      /** Zooms out until limits reached. */
      plot.zoomOutMax = function() {
        for(var i = 0; i < 100 && plot.isZoomed(); i++)
          plot.zoomOut();
      }
    }
    
    $.plot.plugins.push({
        init: init,
        options: {},
        name: 'navigate-utils',
        version: '1.3'
    });
})(jQuery);
