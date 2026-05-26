import { useState } from "react";
import AddOutlinedIcon from "@mui/icons-material/AddOutlined";
import DeleteOutlinedIcon from "@mui/icons-material/DeleteOutlined";
import Box from "@mui/material/Box";
import FormControl from "@mui/material/FormControl";
import Grid from "@mui/material/Grid";
import IconButton from "@mui/material/IconButton";
import InputLabel from "@mui/material/InputLabel";
import MenuItem from "@mui/material/MenuItem";
import Select from "@mui/material/Select";
import Typography from "@mui/material/Typography";
import { FormattedMessage } from "react-intl";
import Button from "components/Button";
import ButtonLoader from "components/ButtonLoader";
import FormButtonsWrapper from "components/FormButtonsWrapper";
import { MANAGER, ENGINEER, MEMBER, SCOPE_TYPES } from "utils/constants";
import { SPACING_1 } from "utils/layouts";

const ORG_ROLE_OPTIONS = [
  { value: MEMBER, messageId: "member" },
  { value: ENGINEER, messageId: "organizationEngineer" },
  { value: MANAGER, messageId: "organizationManager" },
];

const POOL_ROLE_OPTIONS = [
  { value: MANAGER, messageId: "manager" },
  { value: ENGINEER, messageId: "engineer" },
  { value: MEMBER, messageId: "member" },
];

type Assignment = {
  purpose: string;
  assignment_resource_id: string;
  assignment_resource_type: string;
};

type Pool = {
  id: string;
  name: string;
  pool_purpose: string;
};

type PoolRole = {
  poolId: string;
  role: string;
  assignmentId?: string; // Used to track if this is an existing assignment
};

type EditEmployeeRoleFormProps = {
  employee: {
    id: string;
    name: string;
    user_email: string;
    assignments?: Array<Assignment>;
  };
  organizationId: string;
  organizationName: string;
  availablePools: Pool[];
  isGetAvailablePoolsLoading: boolean;
  onSubmit: (data: { organizationRole: string; poolRoles: PoolRole[] }) => void;
  onCancel: () => void;
  isLoading?: boolean;
};

const EditEmployeeRoleForm = ({
  employee,
  organizationId,
  organizationName,
  availablePools,
  isGetAvailablePoolsLoading,
  onSubmit,
  onCancel,
  isLoading = false,
}: EditEmployeeRoleFormProps) => {
  // Find the current organization role
  const currentOrgAssignment = employee.assignments?.find(
    (a) => a.assignment_resource_id === organizationId && a.assignment_resource_type === SCOPE_TYPES.ORGANIZATION
  );
  const currentOrgRole = currentOrgAssignment?.purpose || MEMBER;

  // Find current pool roles
  const currentPoolAssignments =
    employee.assignments?.filter((a) => a.assignment_resource_type === SCOPE_TYPES.POOL).map((a) => ({
      poolId: a.assignment_resource_id,
      role: a.purpose,
      assignmentId: a.assignment_resource_id, // Track existing assignments
    })) || [];

  const [organizationRole, setOrganizationRole] = useState(currentOrgRole);
  const [poolRoles, setPoolRoles] = useState<PoolRole[]>(currentPoolAssignments);

  const addPoolRole = () => {
    setPoolRoles([...poolRoles, { poolId: "", role: MANAGER }]);
  };

  const removePoolRole = (index: number) => {
    setPoolRoles(poolRoles.filter((_, i) => i !== index));
  };

  const updatePoolRole = (index: number, field: "poolId" | "role", value: string) => {
    const newPoolRoles = [...poolRoles];
    newPoolRoles[index] = { ...newPoolRoles[index], [field]: value };
    setPoolRoles(newPoolRoles);
  };

  const usedPoolIds = poolRoles.map((pr) => pr.poolId).filter((id) => id);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    onSubmit({
      organizationRole,
      poolRoles: poolRoles.filter((pr) => pr.poolId && pr.role), // Only submit complete pool roles
    });
  };

  const hasChanges =
    organizationRole !== currentOrgRole ||
    JSON.stringify(poolRoles.sort((a, b) => a.poolId.localeCompare(b.poolId))) !==
      JSON.stringify(currentPoolAssignments.sort((a, b) => a.poolId.localeCompare(b.poolId)));

  return (
    <form onSubmit={handleSubmit} noValidate>
      <Box sx={{ mb: 3 }}>
        <Typography variant="body1" gutterBottom>
          <FormattedMessage id="employee" />: <strong>{employee.name}</strong>
        </Typography>
        <Typography variant="body2" color="text.secondary" gutterBottom>
          {employee.user_email}
        </Typography>
      </Box>

      {/* Organization Role */}
      <Typography variant="h6" gutterBottom>
        <FormattedMessage id="organizationRole" />
      </Typography>
      <Grid container spacing={SPACING_1} sx={{ mb: 2 }}>
        <Grid item xs={6}>
          <FormControl fullWidth>
            <InputLabel id="org-role-select-label" shrink>
              <FormattedMessage id="role" />
            </InputLabel>
            <Select
              labelId="org-role-select-label"
              value={organizationRole}
              label={<FormattedMessage id="role" />}
              onChange={(e) => setOrganizationRole(e.target.value)}
              data-test-id="select_org_role"
              notched
            >
              {ORG_ROLE_OPTIONS.map((option) => (
                <MenuItem key={option.value} value={option.value} data-test-id={`option_org_${option.value}`}>
                  <FormattedMessage id={option.messageId} />
                </MenuItem>
              ))}
            </Select>
          </FormControl>
        </Grid>
        <Grid item xs={6}>
          <FormControl fullWidth>
            <InputLabel id="org-label" shrink>
              <FormattedMessage id="organization" />
            </InputLabel>
            <Select
              labelId="org-label"
              disabled
              value={organizationId}
              label={<FormattedMessage id="organization" />}
              notched
            >
              <MenuItem value={organizationId}>{organizationName}</MenuItem>
            </Select>
          </FormControl>
        </Grid>
      </Grid>

      {/* Pool Roles */}
      <Box sx={{ display: "flex", justifyContent: "space-between", alignItems: "center", mb: 1 }}>
        <Typography variant="h6">
          <FormattedMessage id="poolRoles" />
        </Typography>
        <Button
          messageId="addRole"
          dataTestId="btn_add_pool_role"
          onClick={addPoolRole}
          startIcon={<AddOutlinedIcon />}
          size="small"
          variant="outlined"
        />
      </Box>

      {poolRoles.map((poolRole, index) => (
        <Grid container spacing={SPACING_1} key={index} sx={{ mb: 2 }}>
          <Grid item xs={5}>
            <FormControl fullWidth>
              <InputLabel id={`pool-role-select-label-${index}`} shrink>
                <FormattedMessage id="role" />
              </InputLabel>
              <Select
                labelId={`pool-role-select-label-${index}`}
                value={poolRole.role}
                label={<FormattedMessage id="role" />}
                onChange={(e) => updatePoolRole(index, "role", e.target.value)}
                data-test-id={`select_pool_role_${index}`}
                notched
              >
                {POOL_ROLE_OPTIONS.map((option) => (
                  <MenuItem key={option.value} value={option.value} data-test-id={`option_pool_${option.value}_${index}`}>
                    <FormattedMessage id={option.messageId} />
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          </Grid>
          <Grid item xs={6}>
            <FormControl fullWidth disabled={isGetAvailablePoolsLoading}>
              <InputLabel id={`pool-select-label-${index}`} shrink>
                <FormattedMessage id="pool" />
              </InputLabel>
              <Select
                labelId={`pool-select-label-${index}`}
                value={poolRole.poolId}
                label={<FormattedMessage id="pool" />}
                onChange={(e) => updatePoolRole(index, "poolId", e.target.value)}
                data-test-id={`select_pool_${index}`}
                notched
              >
                {availablePools.map((pool) => (
                  <MenuItem
                    key={pool.id}
                    value={pool.id}
                    disabled={usedPoolIds.includes(pool.id) && pool.id !== poolRole.poolId}
                    data-test-id={`option_pool_${pool.id}`}
                  >
                    {pool.name}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          </Grid>
          <Grid item xs={1} sx={{ display: "flex", alignItems: "center" }}>
            <IconButton onClick={() => removePoolRole(index)} color="error" data-test-id={`btn_remove_pool_role_${index}`}>
              <DeleteOutlinedIcon />
            </IconButton>
          </Grid>
        </Grid>
      ))}

      <FormButtonsWrapper>
        {/* @ts-ignore - ButtonLoader typing issue */}
        <ButtonLoader
          messageId="save"
          dataTestId="btn_save_role"
          color="primary"
          variant="contained"
          type="submit"
          isLoading={isLoading}
          disabled={!hasChanges}
          tooltip={{
            show: !hasChanges,
            messageId: "noChangesToSave",
          }}
        />
        <Button messageId="cancel" dataTestId="btn_cancel" onClick={onCancel} />
      </FormButtonsWrapper>
    </form>
  );
};

export default EditEmployeeRoleForm;
