/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. Licensed under a commercial license.
 * You may not use this file except in compliance with the commercial license.
 */

import styled from 'styled-components';

import BadgeComponent from 'modules/components/Badge';
import SplitPane from 'modules/components/SplitPane';
import Panel from 'modules/components/Panel';
import {COLLAPSABLE_PANEL_MIN_WIDTH} from 'modules/components/CollapsablePanel/styled';
import ExpandButton from 'modules/components/ExpandButton';
import EmptyMessage from './EmptyMessage';

import {HEADER_HEIGHT} from './../Header/styled';

export const Instances = styled.main`
  height: calc(100vh - ${HEADER_HEIGHT}px);
  position: relative;
`;

export const Content = styled.div`
  display: flex;
  flex-direction: row;
  position: absolute;
  top: 0;
  left: 0;
  height: 100%;
  width: calc(100% - ${COLLAPSABLE_PANEL_MIN_WIDTH});
`;

export const Filters = styled.div`
  margin-right: 1px;
`;

export const Center = styled(SplitPane)`
  width: 100%;
`;

export const Pane = styled(SplitPane.Pane)`
  border-radius: 3px 3px 0 0;
`;

export const PaneHeader = styled(SplitPane.Pane.Header)`
  border-radius: 3px 3px 0 0;
`;
export const Selections = styled.div`
  width: 479px;
  display: flex;
  margin-left: 1px;
`;

export const RightExpandButton = styled(ExpandButton)`
  position: absolute;
  left: 0;
  top: 0;
  border-top: none;
  border-bottom: none;
  border-left: none;
`;

export const SelectionHeader = styled(Panel.Header)`
  display: flex;
  justify-content: flex-start;

  padding-left: 45px;
  display: flex;
  flex-shrink: 0;
`;

export const Badge = styled(BadgeComponent)`
  top: 2px;
  margin-left: 13px;
`;

export const EmptyMessageWrapper = styled.div`
  position: relative;
`;

export const DiagramEmptyMessage = styled(EmptyMessage)`
  position: absolute;
  height: 100%;
  width: 100%;
  left: 0;
  top: 0;
`;
