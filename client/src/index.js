/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. Licensed under a proprietary license.
 * See the License.txt file for more information. You may not use this file
 * except in compliance with the proprietary license.
 */

import React from 'react';
import ReactDOM from 'react-dom';

import './style.scss';
import 'polyfills';

import {loadConfig} from 'config';

import App from './App';

loadConfig();

ReactDOM.render(<App />, document.getElementById('root'));
