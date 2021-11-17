/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. Licensed under a commercial license.
 * You may not use this file except in compliance with the commercial license.
 */

import React from 'react';
import {HashRouter as Router, Route, Switch, matchPath} from 'react-router-dom';
import {initTranslation} from 'translation';
import {initTracking} from './modules/tracking';

import {
  PrivateRoute,
  Home,
  Collection,
  Report,
  Dashboard,
  Analysis,
  Events,
  Process,
  Sharing,
  License,
  WithLicense,
  Logout,
} from './components';

import {ErrorBoundary, LoadingIndicator, ErrorPage, Button} from 'components';

import {Notifications} from 'notifications';
import {SaveGuard} from 'saveGuard';
import {Prompt} from 'prompt';
import PageViewTracker from 'tracking/PageViewTracker';

import {Provider as Theme} from 'theme';
import {withErrorHandling, UserProvider, DocsProvider} from 'HOC';

class App extends React.Component {
  state = {
    initializationDone: false,
  };

  async componentDidMount() {
    this.props.mightFail(
      (async () => {
        await initTranslation();
        await initTracking();
      })(),
      () => this.setState({initializationDone: true})
    );
  }

  renderEntity = (props) => {
    const components = {
      report: Report,
      dashboard: Dashboard,
      'events/processes': Process,
      collection: Collection,
    };
    const entities = ['report', 'dashboard', 'collection', 'events/processes'];
    let Component, newProps;
    for (let entity of entities) {
      const splitResult = props.location.pathname.split('/' + entity)[1];
      if (splitResult) {
        const match = matchPath(splitResult, {path: '/:id/:viewMode?'});
        newProps = {
          ...props,
          match,
        };
        Component = components[entity];
        break;
      }
    }
    return <Component {...newProps} />;
  };

  render() {
    if (this.props.error) {
      return (
        <ErrorPage
          noLink
          text="Optimize could not be loaded, please make sure the server is running"
        >
          <Button link onClick={() => window.location.reload(true)}>
            Reload
          </Button>
        </ErrorPage>
      );
    }

    if (!this.state.initializationDone) {
      return <LoadingIndicator />;
    }

    return (
      <Theme>
        <Router getUserConfirmation={SaveGuard.getUserConfirmation}>
          <WithLicense>
            <div className="Root-container">
              <ErrorBoundary>
                <DocsProvider>
                  <UserProvider>
                    <Switch>
                      <PrivateRoute exact path="/" component={Home} />
                      <PrivateRoute path="/analysis" component={Analysis} />
                      <PrivateRoute exact path="/events/processes" component={Events} />
                      <PrivateRoute path="/events/ingested" component={Events} />
                      <Route exact path="/share/:type/:id" component={Sharing} />
                      <PrivateRoute
                        path="/(report|dashboard|collection|events/processes)/*"
                        render={this.renderEntity}
                      />
                      <Route path="/license" component={License} />
                      <Route path="/logout" component={Logout} />
                      <PrivateRoute path="*" component={ErrorPage} />
                    </Switch>
                  </UserProvider>
                </DocsProvider>
              </ErrorBoundary>
            </div>
          </WithLicense>
          <SaveGuard />
          <Prompt />
          <PageViewTracker />
        </Router>
        <Notifications />
      </Theme>
    );
  }
}

export default withErrorHandling(App);
