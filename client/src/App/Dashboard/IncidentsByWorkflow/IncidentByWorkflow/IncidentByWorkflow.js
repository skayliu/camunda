/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. Licensed under a commercial license.
 * You may not use this file except in compliance with the commercial license.
 */

import React from 'react';
import PropTypes from 'prop-types';
import * as Styled from './styled.js';

function IncidentStatistic(props) {
  const {label, activeCount, incidentsCount} = props;
  const incidentsBarRatio =
    (100 * incidentsCount) / (activeCount + incidentsCount);

  return (
    <div className={props.className}>
      <Styled.Wrapper perUnit={props.perUnit}>
        <Styled.IncidentsCount>{incidentsCount}</Styled.IncidentsCount>
        <Styled.Label>{label}</Styled.Label>
        <Styled.ActiveCount>{activeCount}</Styled.ActiveCount>
      </Styled.Wrapper>
      <Styled.Bar>
        <Styled.IncidentsBar
          style={{
            width: `${incidentsBarRatio}%`
          }}
        />
      </Styled.Bar>
    </div>
  );
}

IncidentStatistic.propTypes = {
  label: PropTypes.string.isRequired,
  activeCount: PropTypes.number.isRequired,
  incidentsCount: PropTypes.number.isRequired,
  className: PropTypes.string,
  perUnit: PropTypes.bool
};

export default IncidentStatistic;
