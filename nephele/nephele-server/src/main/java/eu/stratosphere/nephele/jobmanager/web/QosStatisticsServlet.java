/**
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

package eu.stratosphere.nephele.jobmanager.web;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import eu.stratosphere.nephele.jobgraph.JobID;

/**
 * A Servlet that caches and serves qos statistics as JSON.
 * 
 * @author Sascha Wolke
 */
public class QosStatisticsServlet extends HttpServlet {

	private static final long serialVersionUID = 250891613311115346L;

	private static final Log LOG = LogFactory.getLog(QosStatisticsServlet.class);

	private static final ConcurrentHashMap<JobID, JobStatistic> jobStatistics = new ConcurrentHashMap<JobID, JobStatistic>();

	private static JobID lastStartedJob = null;

	private static long INITIAL_REFRESH_INTERVAL = 1000;

	public QosStatisticsServlet() {
	}

	public static abstract class JobStatistic {
		public abstract JobID getJobId();

		public abstract long getCreationTimestamp();

		public abstract JSONObject getJobMetadata() throws JSONException;

		public abstract JSONObject getStatistics(JSONObject jobJson) throws JSONException;

		public abstract JSONObject getStatistics(JSONObject jobJson, long startTimestamp) throws JSONException;

		public abstract long getRefreshInterval();

		public abstract int getMaxEntriesCount();
	}

	public static void putStatistic(JobID jobId, JobStatistic statistics) {
		jobStatistics.put(jobId, statistics);
		lastStartedJob = jobId;
	}

	public static void removeJob(JobID jobId) {
		jobStatistics.remove(jobId);

		if (lastStartedJob == jobId)
			lastStartedJob = null;
	}

	private JobID getLastCreatedJob() {
		if (lastStartedJob != null) {
			return lastStartedJob;
		} else {
			JobID lastCreated = null;
			long ts = -1;

			for (JobStatistic job : jobStatistics.values()) {
				if (job.getCreationTimestamp() > ts) {
					lastCreated = job.getJobId();
					ts = job.getCreationTimestamp();
				}
			}

			return lastCreated;
		}
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		try {
			resp.setStatus(HttpServletResponse.SC_OK);
			resp.setContentType("application/json");

			JSONObject jobs = new JSONObject();
			JobID job;
			long startTimestamp = -1;

			if (req.getParameter("job") != null && !req.getParameter("job").isEmpty())
				job = JobID.fromHexString(req.getParameter("job"));
			else
				job = getLastCreatedJob();

			if (req.getParameter("startTimestamp") != null && !req.getParameter("startTimestamp").isEmpty())
				startTimestamp = Long.parseLong(req.getParameter("startTimestamp"));

			for (JobID id : jobStatistics.keySet()) {
				JSONObject jobDetails = jobStatistics.get(id).getJobMetadata();

				if (job != null && job.equals(id)) {
					if (startTimestamp > 0)
						jobStatistics.get(id).getStatistics(jobDetails, startTimestamp);
					else
						jobStatistics.get(id).getStatistics(jobDetails);
				}

				jobs.put(id.toString(), jobDetails);
			}

			JSONObject result = new JSONObject();
			if (job != null && jobStatistics.containsKey(job)) {
				result.put("currentJob", job);
				result.put("refreshInterval", jobStatistics.get(job).getRefreshInterval());
				result.put("maxEntriesCount", jobStatistics.get(job).getMaxEntriesCount());
			} else {
				result.put("refreshInterval", INITIAL_REFRESH_INTERVAL);
				result.put("maxEntriesCount", 0);
			}
			result.put("jobs", jobs);
			result.write(resp.getWriter());

		} catch (JSONException e) {
			LOG.error("JSON Error: " + e.getMessage(), e);
			resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
			resp.setContentType("application/json");
			resp.getWriter().println("{ status: \"internal error\", message: \"" + e.getMessage() + "\" }");
		}
	}
}
