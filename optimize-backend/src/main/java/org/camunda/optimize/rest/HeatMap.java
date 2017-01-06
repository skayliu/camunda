package org.camunda.optimize.rest;

import org.camunda.optimize.dto.HeatMapRequestTO;
import org.camunda.optimize.service.HeatMapService;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.Map;

/**
 * @author Askar Akhmerov
 */
@Path("/heatmap")
public class HeatMap {

  private HeatMapService heatMapService = new HeatMapService();

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Map<String, Long> getHeatMap(HeatMapRequestTO to) {
    return heatMapService.getHeatMap(to.getKey());
  }


  @POST
  @Path("/correlation")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Long getCorrelation(HeatMapRequestTO to) {
    return heatMapService.activityCorrelation(to.getKey(), to.getCorrelationActivities());
  }
}
