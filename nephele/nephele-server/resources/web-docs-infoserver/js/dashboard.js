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

/* Poll jobs finished, canceld and failed count. */
$(pollVersion);
$(pollTaskmanagerCount);
$(pollJobArchive);

var jsonGlobal;
var widthProgressbar = 120;

// For coloring the dependency graph
var colors = [ "#37485D", "#D9AADC", "#4F7C61", "#8F9C6A", "#BC8E88" ];

var timestamp = 0;

var recentjobs = new Array();

/*
 * Initializes global job table
 */
function init() {
    $(".poll-job-loading-icon").fadeIn();
	$.getJSON('jobsInfo', function(json) {
		// If no job running, poll for new jobs
		if(json == "")
			setTimeout(init, 2000);
		
	    jsonGlobal = json
	    // Fill Table	
	    fillTable("#jobs", json)
	})
    .always(function() { $(".poll-job-loading-icon").fadeOut(); });
}
	
// Init once on page load
$(init());

/*
 * Pools for updates on currently running jobs
 */
function poll(jobId) {
    $(".poll-job-loading-icon").fadeIn();
    $.getJSON('jobsInfo', { 'get': 'updates', 'job': jobId }, function(json) {
        // Call init of no more jobs are running
        $.each(json.recentjobs, function(j, job) {
            if(!$.inArray(job.jobid, recentjobs)) {
                init();
            }
        });
        
        updateTable(json);
    })
    .always(function() { $(".poll-job-loading-icon").fadeOut(); });
}


/*
 * Polls the number of taskmanagers and auto reload every five seconds.
 */
function pollTaskmanagerCount() {
    $.getJSON('jobsInfo', { 'get': 'taskmanagers' })
        .success(function(json) {
          $("#stat-taskmanagers").html(json.taskmanagers);
        })
        .always(setTimeout(pollTaskmanagerCount, 5000));
}

/*
 * Toggle ExecutionVectors
 */
$(document).on("click", ".opensub", function() {
	var id = $(this).data("open");
	$("#" + id).toggle();
	drawDependencies();
});

/**
 * Cancels a job
 */
$(document).on("click", ".btn-cancel-job", function() {
  $.ajax({
    url: 'jobsInfo',
    data: { get: 'cancel', job: $(this).data('job') },
    success: function(json) { /* TODO reload */ }
  });
});

/*
 * Scale nodes in group vertex up or down.
 */
$(document).on("click", ".btn-scale-groupvertex", function() {
  $.ajax({
      url: 'scale',
      data: {
          'job': $(this).data('job'),
          'groupVertex': $(this).data('groupvertex'),
          'groupVertexName': $(this).data('groupvertex-name'),
          'mode': $(this).data('mode'),
          'count': $(this).data('count')
      },
      // TODO: reload: success: function() { ... },
      error: function(jqXHR, textStatus, errorThrown) {
          alert("Scaling failure: " + jqXHR.responseText); 
      }
  }); 
});

/*
 * Draw graph on left side beside table
 */
function drawDependencies() {
	$.each(jsonGlobal, function(i, job) {
		$("#dependencies" + job.jobid).clearCanvas();
		$("#dependencies" + job.jobid).attr("height", 10);
		$("#dependencies" + job.jobid).attr("height", ($("#" + job.jobid).height()-$("#sum").height()-14));

		edgeCount = -1;
		$.each(job.groupvertices, function(j, groupvertex) {
			$.each(groupvertex.backwardEdges, function(k, edge) {
				var y1 = ($("#" + edge.groupvertexid).offset().top - $("#dependencies"+ job.jobid).offset().top) + 15;
				var y2 = ($("#" + groupvertex.groupvertexid).offset().top - $("#dependencies" + job.jobid).offset().top) + 15;
				var cy1 = y1 + (y2 - y1) / 2;
				var cx1 = 0;
				edgeCount++;

				var strokeWidth = 2;
				if (edge.channelType == "NETWORK")
					var strokeWidth = 3;

				$("#dependencies" + job.jobid).drawQuadratic({
					strokeStyle : colors[edgeCount % 5],
					strokeWidth : strokeWidth,
					rounded : true,
					endArrow : true,
					arrowRadius : 10,
					arrowAngle : 40,
					x1 : 95,
					y1 : y1, // Start point
					cx1 : cx1,
					cy1 : cy1, // Control point
					x2 : 95,
					y2 : y2 // End point
				});
			});
		});
	});

}

/*
 * Creates and fills the global running jobs table
 */
function fillTable(table, json) {
	$(table).html("");

	$.each(json, function(i, job) {
		$("#rJpH").hide();
		var countGroups = 0;
		var countTasks = 0;
		var countStarting = 0;
		var countRunning = 0;
		var countSuspending = 0;
		var countSuspended = 0;
		var countFinished = 0;
		var countCanceled = 0;
		var countFailed = 0;
		recentjobs.push(job.jobid);
		poll(job.jobid);

		if(parseInt(job.time) > timestamp)
			timestamp = parseInt(job.time);

		$(table).append("<h2 id=\""+job.jobid+"_title\">"+ job.jobname + " ("+ formattedTimeFromTimestamp(job.time) + ")</h2>");
        $(table).append('<p>'
                + '<a id="' + job.jobid + '_qos" class="btn btn-info" href="qos-statistics.html?job=' + job.jobid +'">'
                  + '<i class="fa fa-area-chart"></i> QoS statistics</a>'
                + '<a id="' + job.jobid + '_cancel" class="btn-cancel-job btn btn-warning pull-right" href="#" data-job="' + job.jobid + '">'
                  + '<i class="fa fa-eraser"></i> Cancel job</a>'
            + '</p>');


		var jobtable;
		jobtable = "<div class=\"table-responsive\">";
		jobtable += "<table class=\"table table-bordered table-hover table-striped\" id=\""+job.jobid+"\" jobname=\""+job.jobname+"\">\
						<tr> \
							<th>Name</th> \
							<th>Tasks</th> \
							<th>Starting</th> \
							<th>Running</th> \
							<th>Suspending</th> \
							<th>Suspended</th> \
							<th>Finished</th> \
							<th>Canceled</th> \
							<th>Failed</th> \
						</tr>";

		$.each(job.groupvertices, function(j, groupvertex) {
			countGroups++;
			countTasks += groupvertex.numberofgroupmembers;
			starting = (groupvertex.CREATED + groupvertex.SCHEDULED + groupvertex.ASSIGNED + groupvertex.READY + groupvertex.STARTING);
			countStarting += starting;
			countRunning += groupvertex.RUNNING;
			countSuspending += groupvertex.SUSPENDING;
			countSuspended += groupvertex.SUSPENDED;
			countFinished += groupvertex.FINISHING + groupvertex.FINISHED;
			countCanceled += groupvertex.CANCELING + groupvertex.CANCELED;
			countFailed += groupvertex.FAILED;
			jobtable += "<tr data-groupvertex=\"" + groupvertex.groupvertexid + "\">\
							<td id=\""+groupvertex.groupvertexid+"\">\
								<span class=\"opensub\" data-open=\"_"+groupvertex.groupvertexid+"\">"
									+ groupvertex.groupvertexname
								+ "</span>\
							</td>\
							<td class=\"nummembers\">"+ groupvertex.numberofgroupmembers+ "</td>";
			jobtable += progressBar(groupvertex.numberofgroupmembers, starting, 'starting');
			jobtable += progressBar(groupvertex.numberofgroupmembers, groupvertex.RUNNING, 'running');
			jobtable += progressBar(groupvertex.numberofgroupmembers, groupvertex.SUSPENDING, 'suspending');
			jobtable += progressBar(groupvertex.numberofgroupmembers, groupvertex.SUSPENDED, 'suspended');
			jobtable += progressBar(groupvertex.numberofgroupmembers, (groupvertex.FINISHING + groupvertex.FINISHED), 'success finished');
			jobtable += progressBar(groupvertex.numberofgroupmembers, (groupvertex.CANCELING + groupvertex.CANCELED), 'warning canceled');
			jobtable += progressBar(groupvertex.numberofgroupmembers, groupvertex.FAILED, 'danger failed');
			jobtable +=	"</tr><tr> \
						<td colspan=9 id=\"_"+groupvertex.groupvertexid+"\" style=\"display:none\"> \
                                " + createScaleButtonGroup(job, groupvertex) + " \
								<div class =\"table-responsive\"> \
								<table class=\"table table-bordered table-hover table-striped\"> \
							  	<tr> \
							  		<th>Name</th> \
							  		<th>status</th> \
							  		<th>instancename</th> \
							  		<th>instancetype</th> \
							  	</tr>";
							$.each(groupvertex.groupmembers, function(k, vertex) {
								jobtable += "<tr id=\"" + vertex.vertexid + "\" data-groupvertex=\"" + groupvertex.groupvertexid + "\" data-vertex=\"" + vertex.vertexid + "\" lastupdate=\""+job.time+"\">\
       								<td>"+ vertex.vertexname + "</td>\
       								<td class=\"status\">"+ vertex.vertexstatus + "</td>\
       								<td>"+ vertex.vertexinstancename + "</td>\
       								<td>"+ vertex.vertexinstancetype + "</td>\
       							</tr>";
							});
							jobtable += "</table></div>\
						</td></tr>";
		});

		jobtable += "<tr id=\"sum\">\
						<td colspan=\"2\" align=\"center\">Sum</td>\
						<td class=\"nummebembers\">"+ countTasks + "</td>";
		jobtable += progressBar(countTasks, countStarting, 'starting');
		jobtable += progressBar(countTasks, countRunning, 'running');
		jobtable += progressBar(countTasks, countSuspending, 'suspending');
		jobtable += progressBar(countTasks, countSuspended, 'suspended');
		jobtable += progressBar(countTasks, countFinished, 'success finished');
		jobtable += progressBar(countTasks, countCanceled, 'warning canceled');
		jobtable += progressBar(countTasks, countFailed, 'danger failed');
		jobtable += "</tr>";

		jobtable += "</table></div>"
		$(table).append(jobtable);
		$("#" + job.jobid).prepend(
						"<tr><td width=\"100\" rowspan=" + (countGroups * 2 + 2)+ " style=\"overflow:hidden\">\
							<canvas id=\"dependencies" + job.jobid+ "\" height=\"10\" width=\"100\"></canvas>\
						</td></tr>");
	});
	drawDependencies(json);

}

/*
 * Generates a button group with scale up and down buttons.
 */
function createScaleButtonGroup(job, groupvertex) {
    var maxUp = groupvertex.SUSPENDED;
    var maxDown = groupvertex.RUNNING - 1; // keep at least one running

    return '<div class="btn-toolbar text-center" role="toolbar">'
        + '<div class="btn-group">'
          + createScaleButton(job, groupvertex, 'up', maxUp, 1)
          + createScaleButton(job, groupvertex, 'up', maxUp, 5)
          + createScaleButton(job, groupvertex, 'up', maxUp, 10)
        + '</div>'
        + '<div class="btn-group">'
          + createScaleButton(job, groupvertex, 'down', maxDown, 1)
          + createScaleButton(job, groupvertex, 'down', maxDown, 5)
          + createScaleButton(job, groupvertex, 'down', maxDown, 10)
        + '</div></div>';
}

function createScaleButton(job, groupvertex, mode, max, count) {
    return '<a'
          + ' class="btn btn-info btn-scale-groupvertex' + ((count > max) ? ' disabled' : '') + '"'
          + ' data-job="' + job.jobid + '"'
          + ' data-groupvertex="' + groupvertex.groupvertexid + '"'
          + ' data-groupvertex-name="' + groupvertex.groupvertexname + '"'
          + ' data-mode="' + mode + '"'
          + ' data-count="' + count + '">'
        + '<i class="fa fa-arrow-circle-' + mode + '"></i>'
        + ' scale ' + mode + ' (' + count + ')</a>';
}

function updateScaleButtons(groupvertexId) {
    var max = {
        up: $('tr[data-groupvertex="' + groupvertexId + '"] .suspended').attr('val'),
        down: $('tr[data-groupvertex="' + groupvertexId + '"] .running').attr('val')
    };
    $.each(['up', 'down'], function(_, mode) {
        $.each([1, 5, 10], function (_, count) {
            var btn = $('.btn-scale-groupvertex[data-groupvertex="' + groupvertexId + '"][data-mode="' + mode + '"][data-count="' + count + '"]');
            if (count > max[mode])
                btn.addClass('disabled');
            else
                btn.removeClass('disabled');
        });
    });
}

/*
 * Generates the progress bars
 */
function progressBar(maximum, input, classVal) {
	if (input != 0) {
        var value = (input / maximum) * 100;

		return '<td class="' + classVal + '" val="' + input + '"> \
            <div class="progress"> \
                <div class="progress-bar progress-bar-success" role="progressbar" \
                    aria-valuemin="0" aria-valuemax="100" aria-valuenow="' + value + '" style="width: ' + value + '%"> \
                        ' + input + ' \
            </div></div></td>';
	} else {
		return "<td class=\""+classVal+"\" val=\""+input+"\">"+input+"</td>";
	}
}


/*
 * Generates the progress bar without the class val
 */
function progressBar2(maximum, input) {
	if (input != 0) {
		return "<div class=\"progress\"><div class=\"progress-bar progress-bar-success\" role=\"progressbar\""
		 			+ "aria-valuemin=\"0\" aria-valuemax=\""+widthProgressbar+"\" style=\"width:"
					+ (20 + 80*input/maximum) + "%;\">"
					+ input
					+ "</div></div>";
	}
}

/*
 * Returns the new width for a progress bar
 */
function newWidth(maximum, val) {
	return (val / maximum) * 100;
}
	

/*
 * Updates the global running job table with newest events
 */
function updateTable(json) {
	var pollfinished = false;
	$.each(json.vertexevents , function(i, event) {

		if(parseInt($("#"+event.vertexid).attr("lastupdate")) < event.timestamp)
		{
			// not very nice
			var oldstatus = ""+$("#"+event.vertexid).children(".status").html();
			if(oldstatus == "CREATED" ||  oldstatus == "SCHEDULED" ||oldstatus == "ASSIGNED" ||oldstatus == "READY" ||oldstatus == "STARTING")
				oldstatus = "starting";
			else if(oldstatus == "FINISHING")
				oldstatus = "finished";
			else if(oldstatus == "CANCELING")
				oldstatus = "canceled";
			
			var newstate = event.newstate;
			if(newstate == "CREATED" ||  newstate == "SCHEDULED" ||newstate == "ASSIGNED" ||newstate == "READY" ||newstate == "STARTING")
				newstate = "starting";
			else if(newstate == "FINISHING")
				newstate = "finished";
			else if(newstate == "CANCELING")
				newstate = "canceled";
			
			// update detailed state
			$("#"+event.vertexid).children(".status").html(event.newstate);
			// update timestamp
			$("#"+event.vertexid).attr("lastupdate", event.timestamp);
			
			var nummembers = parseInt($("#"+event.vertexid).parent().parent().parent().parent().parent().prev().children(".nummembers").html());
			var summembers = parseInt($("#sum").children(".nummebembers").html());
			

			if(oldstatus.toLowerCase() != newstate.toLowerCase()) {
			
				// adjust groupvertex
				var oldcount = parseInt($("#"+event.vertexid).parent().parent().parent().parent().parent().prev().children("."+oldstatus.toLowerCase()).attr("val"));
				var oldcount2 = parseInt($("#"+event.vertexid).parent().parent().parent().parent().parent().prev().children("."+newstate.toLowerCase()).attr("val"));
				$("#"+event.vertexid).parent().parent().parent().parent().parent().prev().children("."+oldstatus.toLowerCase()).attr("val", oldcount-1);
				$("#"+event.vertexid).parent().parent().parent().parent().parent().prev().children("."+newstate.toLowerCase()).attr("val", oldcount2+1);
				
				//adjust progressbars nummembers
				if (oldcount == 1) {
					$("#"+event.vertexid).parent().parent().parent().parent().parent().prev().children("."+oldstatus.toLowerCase()).children().first().remove();
					$("#"+event.vertexid).parent().parent().parent().parent().parent().prev().children("."+oldstatus.toLowerCase()).html("0");
					
				} else if (oldcount > 1) {
					$("#"+event.vertexid).parent().parent().parent().parent().parent().prev().children("."+oldstatus.toLowerCase()).first().children().first().children().first().css("width", newWidth(nummembers, (oldcount-1))+"%").html(oldcount-1);
				}
				
				if (oldcount2 == 0) {
					$("#"+event.vertexid).parent().parent().parent().parent().parent().prev().children("."+newstate.toLowerCase()).html(progressBar2(nummembers, 1, newstate.toLowerCase()));
				} else if (oldcount2 > 0) {
					$("#"+event.vertexid).parent().parent().parent().parent().parent().prev().children("."+newstate.toLowerCase()).first().children().first().children().first().css("width", newWidth(nummembers, (oldcount2+1))+"%").html(oldcount2+1);
				}			
				// adjust sum
				oldcount = parseInt($("#sum").children("."+oldstatus.toLowerCase()).attr("val"));
				oldcount2 = parseInt($("#sum").children("."+newstate.toLowerCase()).attr("val"));
				$("#sum").children("."+oldstatus.toLowerCase()).attr("val", oldcount-1);
				$("#sum").children("."+newstate.toLowerCase()).attr("val", oldcount2+1);
				
				//adjust progressbars summembers
				if (oldcount == 1) {
					$("#sum").children("."+oldstatus.toLowerCase()).children().first().remove();
					$("#sum").children("."+oldstatus.toLowerCase()).html("0");
				} else if (oldcount > 1) {
					$("#sum").children("."+oldstatus.toLowerCase()).first().children().first().children().first().css("width", newWidth(summembers, (oldcount-1))+"%").html(oldcount-1);
				}
				
				if (oldcount2 == 0) {
					$("#sum").children("."+newstate.toLowerCase()).first().html(progressBar2(summembers, 1, newstate.toLowerCase()));
				} else if (oldcount2 > 0) {
					$("#sum").children("."+newstate.toLowerCase()).first().children().first().children().first().css("width", newWidth(summembers, (oldcount2+1))+"%").html(oldcount2+1);
				}

                // update scale buttons
                updateScaleButtons($('[data-vertex="' + event.vertexid + '"]').data('groupvertex'));
            }
		}
	});
	
	// handle jobevents
	$.each(json.jobevents , function(i, event) {
		if(event.newstate == "FINISHED" || event.newstate == "FAILED" || event.newstate == "CANCELED") {
			// stop polling
			pollfinished = true;

			// delete table
			var jobname = $("#"+json.jobid).attr("jobname");
			$("#"+json.jobid).remove();
			$("#"+json.jobid+"_title").remove();
			$("#"+json.jobid+"_cancel").remove();
			$("#"+json.jobid+"_qos").remove();

			// remove from internal list
			for(var i in recentjobs){
			    if(recentjobs[i]==json.jobid){
			    	recentjobs.splice(i,1);
			        break;
			    }
			}
			
			//display message if all jobs are done
			if(recentjobs.length == 0) {
				$("#rJpH").show();
			}

			// add to history
			setTimeout(function() {
				var jobjson = {};
				jobjson.jobid = json.jobid;
				jobjson.jobname = jobname;
				jobjson.time = event.timestamp;
				jobjson.status = event.newstate;
				_fillTableArchive("#jobsArchive", jobjson, true);
			}, 8000);
		}
	});
	
	if(!pollfinished)
		 setTimeout(poll, 2000, json.jobid);
	else if(recentjobs.length == 0) {
		// wait long enough for job to be completely removed on server side before new init
		setTimeout(init, 5000);
	}
}


function pollJobArchive() {
  $.getJSON('jobsInfo', { 'get': 'archive' })
    .done(function(jobs) {
      var count = { 'finished': 0, 'canceled': 0, 'failed': 0 };
      $.each(jobs, function(i, job) {
        count[job.status.toLowerCase()]++;
      });
      for(var status in count) {
        $('#jobs-' + status + '-count').text(count[status]);
      }
    })
    .always(setTimeout(pollJobArchive, 10000));
}

function pollVersion() {
  $.getJSON('jobsInfo', { 'get': 'version' })
    .done(function(json) {
      if (json.version && json.version != "null") {
        if (json.version.indexOf("SNAPSHOT") > -1) {
            $("#version").text(" " + json.version + " - " + json.revision);
        } else {
            $("#version").text(" " + json.version);
        }
      }
    })
    .fail(function() { setTimeout(pollVersion, 1000); });
}
