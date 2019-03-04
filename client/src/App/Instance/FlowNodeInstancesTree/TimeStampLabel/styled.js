/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. Licensed under a commercial license.
 * You may not use this file except in compliance with the commercial license.
 */

import styled from 'styled-components';
import {Colors, themed, themeStyle} from 'modules/theme';

export const TimeStamp = themed(styled.span`
  margin-left: 14px;
  padding: 2px 4px;
  color: ${({isSelected}) => isSelected && '#fff'};
  background: ${({isSelected}) =>
    isSelected
      ? 'rgba(247, 248, 250, 0.2)'
      : themeStyle({
          dark: Colors.darkScopeLabel,
          light: Colors.lightScopeLabel
        })};

  font-size: 11px;
  border-radius: 2px;
`);
