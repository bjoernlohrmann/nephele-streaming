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

$(pollJobArchive);

function loadJobLists(jobs) {
  var count = { 'finished': 0, 'canceled': 0, 'failed': 0 };

  for(var status in count)
      $("#jobs-" + status + ' .list-group').empty();

  $.each(jobs, function(i, job) {
    var item = $('<a href="analyze.html?job=' + job.jobid + '" id="' + job.jobid + '" class="list-group-item" />');
    item.text(job.jobname);
    item.append('<span class="badge">' + formattedTimeFromTimestamp(parseInt(job.time)) + '</span>');
    $("#jobs-" + job.status.toLowerCase() + ' .list-group').append(item);

    count[job.status.toLowerCase()]++;
  });

  for(var status in count) {
    if (count[status] == 0)
      $("#jobs-" + status + ' .list-group').append('<div class="list-group-item">No jobs found</div>');
  }
}

/* Fetch job archive and auto reload every 10s. */
function pollJobArchive() {
  $.getJSON('jobsInfo', { 'get': 'archive' })
    .done(loadJobLists)
    .always(function () { setTimeout(pollJobArchive, 10000); });
}
