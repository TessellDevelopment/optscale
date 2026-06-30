import { useEffect } from "react";
import { useDispatch } from "react-redux";
import { updateEmployeeRole, deleteEmployeeRole, getAvailablePools } from "api";
import { UPDATE_EMPLOYEE_ROLE, GET_AVAILABLE_POOLS } from "api/restapi/actionTypes";
import EditEmployeeRoleForm from "components/forms/EditEmployeeRoleForm";
import { useApiData } from "hooks/useApiData";
import { useApiState } from "hooks/useApiState";
import { useOrganizationInfo } from "hooks/useOrganizationInfo";
import { isError } from "utils/api";
import { SCOPE_TYPES } from "utils/constants";

type EditEmployeeRoleContainerProps = {
  employee: {
    id: string;
    name: string;
    user_email: string;
    assignments?: Array<{
      purpose: string;
      assignment_resource_id: string;
      assignment_resource_type: string;
    }>;
  };
  organizationId: string;
  closeSideModal: () => void;
};

const EditEmployeeRoleContainer = ({ employee, organizationId, closeSideModal }: EditEmployeeRoleContainerProps) => {
  const { isLoading } = useApiState(UPDATE_EMPLOYEE_ROLE);
  const { name: organizationName } = useOrganizationInfo();
  const dispatch = useDispatch();

  // Fetch available pools
  const { isLoading: isGetAvailablePoolsLoading, shouldInvoke } = useApiState(GET_AVAILABLE_POOLS, { organizationId });

  useEffect(() => {
    if (shouldInvoke) {
      dispatch(getAvailablePools(organizationId));
    }
  }, [dispatch, organizationId, shouldInvoke]);

  const {
    apiData: { pools = [] },
  } = useApiData(GET_AVAILABLE_POOLS);

  const onSubmit = (formData: { organizationRole: string; poolRoles: Array<{ poolId: string; role: string }> }) =>
    // @ts-ignore - Redux thunk typing issue
    dispatch((_, getState) => {
      // Get current organization role
      const currentOrgAssignment = employee.assignments?.find(
        (a) => a.assignment_resource_id === organizationId && a.assignment_resource_type === SCOPE_TYPES.ORGANIZATION
      );
      const currentOrgRole = currentOrgAssignment?.purpose;

      // Get current pool assignments
      const currentPoolAssignments =
        employee.assignments?.filter((a) => a.assignment_resource_type === SCOPE_TYPES.POOL) || [];
      const currentPoolIds = currentPoolAssignments.map((a) => a.assignment_resource_id);
      const newPoolIds = formData.poolRoles.map((pr) => pr.poolId);

      // Find deleted pool roles (existed before but not in new list)
      const deletedPoolIds = currentPoolIds.filter((poolId) => !newPoolIds.includes(poolId));

      // Helper to process requests sequentially
      const processSequentially = (actions) => {
        return actions.reduce((promise, action) => {
          return promise.then(() => action());
        }, Promise.resolve());
      };

      // Build action list
      const actions = [];

      // 1. Update organization role only if it changed
      if (currentOrgRole !== formData.organizationRole) {
        actions.push(() =>
          dispatch(
            updateEmployeeRole(employee.id, {
              rolePurpose: formData.organizationRole,
              scopeId: organizationId,
              scopeType: SCOPE_TYPES.ORGANIZATION,
            })
          )
        );
      }

      // 2. Delete removed pool assignments
      deletedPoolIds.forEach((poolId) => {
        actions.push(() =>
          dispatch(
            deleteEmployeeRole(employee.id, {
              scopeId: poolId,
              scopeType: SCOPE_TYPES.POOL,
            })
          )
        );
      });

      // 3. Update or create pool roles (only if role changed or it's new)
      formData.poolRoles.forEach((poolRole) => {
        const existingAssignment = currentPoolAssignments.find((a) => a.assignment_resource_id === poolRole.poolId);

        // Only update if it's a new pool or the role changed
        if (!existingAssignment || existingAssignment.purpose !== poolRole.role) {
          actions.push(() =>
            dispatch(
              updateEmployeeRole(employee.id, {
                rolePurpose: poolRole.role,
                scopeId: poolRole.poolId,
                scopeType: SCOPE_TYPES.POOL,
              })
            )
          );
        }
      });

      // Execute all actions sequentially
      if (actions.length === 0) {
        closeSideModal();
      } else {
        processSequentially(actions).then(() => {
          if (!isError(UPDATE_EMPLOYEE_ROLE, getState())) {
            closeSideModal();
          }
        });
      }
    });

  return (
    <EditEmployeeRoleForm
      employee={employee}
      organizationId={organizationId}
      organizationName={organizationName}
      availablePools={pools}
      isGetAvailablePoolsLoading={isGetAvailablePoolsLoading}
      onSubmit={onSubmit}
      onCancel={closeSideModal}
      isLoading={isLoading || isGetAvailablePoolsLoading}
    />
  );
};

export default EditEmployeeRoleContainer;
