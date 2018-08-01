import React, {Fragment} from 'react';
import PropTypes from 'prop-types';
import StateIcon from 'modules/components/StateIcon';
import Dropdown from 'modules/components/Dropdown';

import {getWorkflowName} from 'modules/utils/instance';
import {BADGE_TYPE} from 'modules/constants';
import {Down, Right, Batch, Retry} from 'modules/components/Icon';

import * as Styled from './styled.js';

export default class Selection extends React.Component {
  static propTypes = {
    isOpen: PropTypes.bool.isRequired,
    selectionId: PropTypes.number.isRequired,
    instances: PropTypes.arrayOf(
      PropTypes.shape({
        id: PropTypes.string.isRequired,
        state: PropTypes.string.isRequired,
        workflowId: PropTypes.string.isRequired
      }).isRequired
    ).isRequired,
    count: PropTypes.number.isRequired,
    onClick: PropTypes.func.isRequired,
    onRetry: PropTypes.func.isRequired,
    onDelete: PropTypes.func.isRequired
  };

  renderArrowIcon = isOpen => (isOpen ? <Down /> : <Right />);

  renderBody = instances => {
    return instances.map((instance, index) => (
      <Styled.Instance key={index}>
        <StateIcon {...{instance}} />
        <Styled.WorkflowName>{getWorkflowName(instance)}</Styled.WorkflowName>
        <Styled.InstanceId>{instance.id}</Styled.InstanceId>
      </Styled.Instance>
    ));
  };

  renderFooter = (count, numberOfDisplayedInstances) => (
    <Styled.Footer>
      <Styled.MoreInstances>
        {count - numberOfDisplayedInstances + ' more Instances'}
      </Styled.MoreInstances>
    </Styled.Footer>
  );

  renderActions = (onRetry, onDelete) => (
    <Styled.Actions>
      <Styled.DropdownTrigger onClick={evt => evt && evt.stopPropagation()}>
        <Dropdown label={<Batch />}>
          <Dropdown.Option onClick={onRetry}>
            <Retry />
            <Styled.OptionLabel>Retry</Styled.OptionLabel>
          </Dropdown.Option>
        </Dropdown>
      </Styled.DropdownTrigger>
      <Styled.DeleteIcon onClick={onDelete} />
    </Styled.Actions>
  );

  render() {
    const {
      isOpen,
      selectionId,
      onRetry,
      onDelete,
      onClick,
      instances,
      count
    } = this.props;
    const {renderArrowIcon, renderBody, renderActions, renderFooter} = this;
    return (
      <Styled.Selection>
        <Styled.Header {...{onClick, isOpen}}>
          {renderArrowIcon(isOpen)}
          <Styled.Headline>Selection {selectionId + 1}</Styled.Headline>
          <Styled.Badge
            type={
              isOpen ? BADGE_TYPE.OPENSELECTIONHEAD : BADGE_TYPE.SELECTIONHEAD
            }
            badgeContent={count}
          />

          {isOpen && renderActions(onRetry, onDelete)}
        </Styled.Header>
        {isOpen && (
          <Fragment>
            {renderBody(instances, count)}
            {renderFooter(count, instances.length)}
          </Fragment>
        )}
      </Styled.Selection>
    );
  }
}
