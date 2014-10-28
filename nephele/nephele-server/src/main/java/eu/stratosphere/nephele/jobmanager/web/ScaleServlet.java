package eu.stratosphere.nephele.jobmanager.web;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.jobmanager.JobManager;
import eu.stratosphere.nephele.util.StringUtils;

public class ScaleServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;

	/**
	 * The log for this class.
	 */
	private static final Log LOG = LogFactory.getLog(ScaleServlet.class);

	/**
	 * Underlying JobManager
	 */
	private final JobManager jobmanager;

	public ScaleServlet(JobManager jobmanager) {
		this.jobmanager = jobmanager;
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		resp.setStatus(HttpServletResponse.SC_OK);
		resp.setContentType("application/json");

		try {
			JobID jobId = null;
			if (req.getParameter("job") != null)
				jobId = JobID.fromHexString(req.getParameter("job"));
			else if (jobmanager.getRecentJobs().size() > 0)
				jobId = jobmanager.getRecentJobs().get(0).getJobID();
			if (jobId == null)
				throw new RuntimeException("Can't find job!");

			int noOfSubtasks = 1;
			if (req.getParameter("count") != null)
				noOfSubtasks = Integer.parseInt(req.getParameter("count"));

			JobVertexID vertexId = null;
			if (req.getParameter("groupVertex") != null)
				vertexId = JobVertexID.fromHexString(req.getParameter("groupVertex"));
			else if (req.getParameter("groupVertexName") != null)
				vertexId = this.jobmanager.getJobVertexIdByName(jobId, req.getParameter("groupVertexName"));
			else
				throw new RuntimeException("Group vertex name required.");

			if ("up".equals(req.getParameter("mode"))) {
				jobmanager.scaleUpElasticTask(jobId, vertexId, noOfSubtasks);
			} else if ("down".equals(req.getParameter("mode"))) {
				jobmanager.scaleDownElasticTask(jobId, vertexId, noOfSubtasks);
			} else
				throw new RuntimeException("Mode parameter missing.");

			resp.getWriter().print("{ \"result\": \"ok\" }");

		} catch (Exception e) {
			resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			resp.getWriter().print(e.getMessage());
			if (LOG.isWarnEnabled()) {
				LOG.warn(StringUtils.stringifyException(e));
			}
		}
	}
}
