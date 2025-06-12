import './Home.css';

const Home = () => {
    //todo: agegroup filter
    //todo: table breakdown improve
    //todo: drill to detail download

    return (
      <>
        <div class="banner">
          <div class="nav-links">
            <div class="tooltip-container">
              <a href="https://insights.nascop.org">Home</a>
              <div class="tooltip-text">
                Project landing page with summaries of available information
                products
              </div>
            </div>
            <div class="tooltip-container">
              <a href="https://analytics.nascop.org/superset/dashboard/1307">
                Real Time Dashboards
              </a>
              <div class="tooltip-text">
                Dashboards showing continuosly updated data to monitor HIV
                trends, detect gaps, and support immediate public health
                response
              </div>
            </div>
            <div class="tooltip-container">
              <a href="https://maps.nascop.org">Hotspot Maps</a>
              <div class="tooltip-text">
                Maps showing geographic hotspots across various surveillance
                indicators, guiding targeted interventions and resource
                allocation
              </div>
            </div>
            <div class="tooltip-container">
              <a href="https://sentinel.nascop.org/">
                Sentinel Events Pathways
              </a>
              <div class="tooltip-text">
                Dashboards that visualize patient journeys and flag sentinel
                events that point to possible clinical or programmatic gaps
                requiring public health action.
              </div>
            </div>
            <div class="tooltip-container">
              <a href="https://analytics.nascop.org/superset/dashboard/1287/">
                Cohort Dashboards
              </a>
              <div class="tooltip-text">
                Dashboards that track individuals over time, highlighting gaps
                in care, treatment outcomes, and opportunities for public health
                action to improve HIV program performance.
              </div>
            </div>
            <div class="tooltip-container">
              <a href="#">Epidemic Surveillance Report</a>
              <div class="tooltip-text">
                A report summarising public health responses to HIV surveillance
                data, highlighting actions taken, outcomes achieved, and areas
                for improvement to optimise intervention strategies.
              </div>
            </div>
          </div>
        </div>
      </>
    );

}
export default Home
