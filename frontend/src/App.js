import './App.css';
import { BrowserRouter, Route, Switch } from 'react-router-dom';
import Dashboard from "./Dashboard";

function App() {
  return (
      <div className="App">
          <BrowserRouter>
              <Switch>
                  <Route path="/">
                    <Dashboard/>
                  </Route>
                  <Route path="/resources">

                  </Route>
              </Switch>
          </BrowserRouter>
      </div>
  );
}

export default App;

