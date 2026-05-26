import { useMemo } from "react";
import DeleteOutlinedIcon from "@mui/icons-material/DeleteOutlined";
import { FormattedMessage } from "react-intl";
import CaptionedCell from "components/CaptionedCell";
import Table from "components/Table";
import TableCellActions from "components/TableCellActions";
import TableLoader from "components/TableLoader";
import TextWithDataTestId from "components/TextWithDataTestId";
import { useFormatIntervalTimeAgo } from "hooks/useFormatIntervalTimeAgo";
import { useOpenSideModal } from "hooks/useOpenSideModal";
import { ROLE_PURPOSES } from "utils/constants";
import { unixTimestampToDateTime } from "utils/datetime";
import DismissInvitationModal from "../SideModalManager/SideModals/DismissInvitationModal";

const PendingInvitationsTable = ({ invitations, isLoading = false, onDismiss }) => {
  const openSideModal = useOpenSideModal();
  const formatTimeAgo = useFormatIntervalTimeAgo();

  const data = useMemo(() => invitations, [invitations]);

  const columns = useMemo(
    () => [
      {
        header: (
          <TextWithDataTestId dataTestId="lbl_email">
            <FormattedMessage id="email" />
          </TextWithDataTestId>
        ),
        accessorKey: "email",
        defaultSort: "asc",
        cell: ({ row: { original } }) => {
          const { email, created_at: createdAt, ttl } = original;
          const now = Math.floor(Date.now() / 1000);
          const expiresIn = ttl - now;

          return (
            <CaptionedCell
              caption={[
                {
                  key: "sent",
                  node: (
                    <FormattedMessage
                      id="sentTime"
                      values={{
                        value: formatTimeAgo(createdAt, 1),
                      }}
                    />
                  ),
                },
                expiresIn > 0 && {
                  key: "expires",
                  node: (
                    <FormattedMessage
                      id="expiresIn"
                      values={{
                        value: formatTimeAgo(ttl, 1),
                      }}
                    />
                  ),
                },
              ].filter(Boolean)}
            >
              {email}
            </CaptionedCell>
          );
        },
      },
      {
        header: (
          <TextWithDataTestId dataTestId="lbl_invited_by">
            <FormattedMessage id="invitedBy" />
          </TextWithDataTestId>
        ),
        accessorKey: "owner_email",
        cell: ({ row: { original } }) => {
          const { owner_name: ownerName, owner_email: ownerEmail } = original;
          return (
            <CaptionedCell caption={ownerEmail}>
              {ownerName}
            </CaptionedCell>
          );
        },
      },
      {
        header: (
          <TextWithDataTestId dataTestId="lbl_organization">
            <FormattedMessage id="organization" />
          </TextWithDataTestId>
        ),
        accessorKey: "organization",
      },
      {
        header: (
          <TextWithDataTestId dataTestId="lbl_roles">
            <FormattedMessage id="roles" />
          </TextWithDataTestId>
        ),
        accessorKey: "invite_assignments",
        enableSorting: false,
        cell: ({ row: { original } }) => {
          const { invite_assignments: assignments = [] } = original;
          return (
            <div>
              {assignments.map((assignment) => (
                <div key={assignment.id}>
                  <FormattedMessage id={ROLE_PURPOSES[assignment.purpose]} /> -{" "}
                  {assignment.scope_name} ({assignment.scope_type})
                </div>
              ))}
            </div>
          );
        },
      },
      {
        header: (
          <TextWithDataTestId dataTestId="lbl_actions">
            <FormattedMessage id="actions" />
          </TextWithDataTestId>
        ),
        id: "actions",
        enableSorting: false,
        enableHiding: false,
        cell: ({ row: { original, index } }) => {
          const { id, email } = original;
          return (
            <TableCellActions
              items={[
                {
                  key: "dismiss",
                  messageId: "dismiss",
                  icon: <DeleteOutlinedIcon />,
                  color: "error",
                  dataTestId: `btn_dismiss_${index}`,
                  action: () =>
                    openSideModal(DismissInvitationModal, {
                      invitationId: id,
                      email,
                      onSuccess: onDismiss,
                    }),
                },
              ]}
            />
          );
        },
      },
    ],
    [openSideModal, formatTimeAgo, onDismiss]
  );

  return isLoading ? (
    <TableLoader columnsCounter={columns.length} showHeader />
  ) : (
    <Table data={data} columns={columns} withSearch pageSize={50} localization={{ emptyMessageId: "noPendingInvitations" }} />
  );
};

export default PendingInvitationsTable;
