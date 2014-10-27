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

var jsonGlobal;
var widthProgressbar = 120;
 
$(pollArchive);

     
HTTP_GET_VARS=new Array();
  
strGET = document.location.search.substr(1, document.location.search.length);
	
if (strGET != '') {
	gArr = strGET.split('&');
	for (i = 0; i < gArr.length; ++i) {
		v = '';
	vArr = gArr[i].split('=');
		if (vArr.length > 1) {
			v = vArr[1];
		}
		HTTP_GET_VARS[unescape(vArr[0])] = unescape(v);
	}
}

function GET(v) {
	if (!HTTP_GET_VARS[v]) {
		return 'undefined';
	}
	return HTTP_GET_VARS[v];
}

function pollArchive() {
    $.getJSON('jobsInfo', { 'get': 'job', 'job': GET('job') }, function(json) {
        jsonGlobal = json
        analyzeTime(json, false)
    });
};

$(document).on("click", "#stack", function() {
	analyzeTime(jsonGlobal, true);
});

$(document).on("click", "#flow", function() {
	analyzeTime(jsonGlobal, false);
});

function analyzeTime(json, stacked) {
	$.each(json, function(i, job) {
		$("#job_timeline").html("");
		$("#time").html(formattedTimeFromTimestamp(job.SCHEDULED));
		$("#run").html(convertTime(job[job.status] - job.SCHEDULED));
		$("#status").html(job.status);
		$("#jobtitle").html(job.jobname);
		
		// create failed table
		if (job.status == "FAILED") {
			failed = "";
			$.each(job.failednodes, function(j, failednode) {
				failed += "<li>" + failednode.node + "<br/>Error: " + failednode.message + "</li>";
			});
			$("#page-wrapper").append("<div class=\"panel panel-primary\"><div class=\"panel-heading\"><h3 class=\"panel-title\">Failed Nodes" +
									 "</h3></div><div id=\"failednodes\" class=\"panel-body\">" +
									 failed +
									 "</div></div>");

		}

		// create accumulators table
		if($.isArray(job.accumulators) && job.accumulators.length > 0) {
			accuTable = "<div class=\"table-responsive\">" +
					"<table class=\"table table-bordered table-hover table-striped\">" +
					"<tr><td><b>Name</b></td><td><b>Value</b></td></tr>";
			$.each(job.accumulators, function(i, accu) {
				accuTable += "<tr><td>"+accu.name+"</td><td>"+accu.value+"</td></tr>";
			});
			accuTable += "</table></div>";
			$("#accumulators").html(accuTable);
		}

        var data = [];

        if (stacked) {
            data.push({ "start": new Date(job.SCHEDULED), "content": "SCHEDULED", "group": "9999", "className" : "scheduled" });
            data.push({ "start": new Date(job[job.status]), "content": job.status, "group": "9999", "className": "finished" });
        } else {
            data.push({ "start": new Date(job.SCHEDULED), "content": "SCHEDULED", "className" : "scheduled" });
            data.push({ "start": new Date(job[job.status]), "content": job.status, "className": "finished" });
        }

		var i = job.groupvertices.length;
		
		$.each(job.groupvertices, function(j, groupvertex) {
			// check for reasonable starting time
			if (job.groupverticetimes[groupvertex.groupvertexid].STARTED < 8888888888888) {
                var item = {
                    "start": new Date(job.groupverticetimes[groupvertex.groupvertexid].STARTED),
                    "end": new Date(job.groupverticetimes[groupvertex.groupvertexid].ENDED),
                    "content": groupvertex.groupvertexname,
                    "className": "running",
                    "groupvertexid": groupvertex.groupvertexid 
                };

                if (stacked)
                    item['group'] = '' + i;

                data.push(item);
				i--;
			}
		});

		// Instantiate our timeline object.
		var timeline = new links.Timeline(document.getElementById('job_timeline'));

		// Add event listeners
		links.events.addListener(timeline, 'select', function(event) {
            var sel = timeline.getSelection();
            if (sel.length > 0 && sel[0].row < data.length) {
                var id = data[sel[0].row].groupvertexid;
                if (id) // start and end have no id!
                    loadGroupvertex(id);
            }
		});

		// Draw our timeline with the created data and options
		timeline.draw(data, {});
	});
}

function loadGroupvertex(groupvertexid) {
    $.getJSON('jobsInfo', { 'get': 'groupvertex', 'job': GET('job'), 'groupvertex': groupvertexid}, analyzeGroupvertexTime);
}

function analyzeGroupvertexTime(json) {
	$("#vertices").html("");
	var groupvertex = json.groupvertex;
	$("#vertices").append(
					'<h2>'+ groupvertex.groupvertexname
					+ '</h2><br /><div id="pl_'+groupvertex.groupvertexid+'"></div>');
	
    var data = []
	$.each(groupvertex.groupmembers, function(i, vertex) {
        var instancename = vertex.vertexinstancename + "_" + i;
        var items = [];

        items.push({ "startState": "READY", "endState": "STARTING", "description": "ready" });
        items.push({ "startState": "STARTING", "endState": "RUNNING", "description": "starting" });


        if (vertex.vertexstatus == "FINISHED") {
            items.push({ "startState": "RUNNING", "endState": "FINISHING", "description": "running" });
            items.push({ "startState": "FINISHING", "endState": "FINISHED", "description": "finishing" });
        }

        if (vertex.vertexstatus == "CANCELED") {
            items.push({ "startState": "RUNNING", "endState": "CANCELING", "description": "running" });
            items.push({ "startState": "CANCELING", "endState": "CANCELED", "description": "canceling" });
        }

        if (vertex.vertexstatus == "FAILED") {
            items.push({ "startState": "RUNNING", "endState": "FAILED", "description": "running - FAILED" });
        }

        $.each(items, function(j, item) {
            var start = json.verticetimes[vertex.vertexid][item.startState];
            var end = json.verticetimes[vertex.vertexid][item.endState];

            if (start > 0 && end > 0) 
                data.push({
                    "start": new Date(start),
                    "end": new Date(end),
                    "content": item.description,
                    "className": item.description,
                    "group": instancename
                });
        });
	});

	// Instantiate our timeline object.
	var timeline = new links.Timeline(document.getElementById('pl_' + groupvertex.groupvertexid));

	// Draw our timeline with the created data and options
	timeline.draw(data, {});
}
