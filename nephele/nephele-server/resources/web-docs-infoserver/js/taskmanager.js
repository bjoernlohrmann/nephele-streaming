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

$(pollTaskmanagerDetails);

/* Fill task managers table. */
function loadTaskmanagersTable(json) {
  var tbody = $('#task-managers tbody');
  tbody.empty();

  $.each(json.taskmanagers, function(i, tm) {
    tbody.append('<tr>'
      + '<td>' + tm.inetAdress + '</td>'
      + '<td>' + tm.ipcPort + '</td>'
      + '<td>' + tm.dataPort + '</td>'
      + '<td>' + tm.cpuCores + '</td>'
      + '<td>' + tm.physicalMemory + '</td>'
      + '<td>' + tm.freeMemory + '</td>'
      + '</tr>');
  });
}

/* Fetch taskmanagers and auto reload every 10s. */
function pollTaskmanagerDetails() {
  $.getJSON('setupInfo', { 'get': 'taskmanagers' })
    .done(loadTaskmanagersTable)
    .always(function() { setTimeout(pollTaskmanagerDetails, 10000); });
}
