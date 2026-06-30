import { NetworkStatus } from "@apollo/client";
import PendingInvitationsTable from "components/PendingInvitationsTable";
import { useOrganizationInvitationsQuery } from "graphql/__generated__/hooks/restapi";
import { useOrganizationInfo } from "hooks/useOrganizationInfo";

const PendingInvitationsContainer = () => {
  const { organizationId } = useOrganizationInfo();

  const {
    data: { organizationInvitations = [] } = {},
    networkStatus,
    refetch,
  } = useOrganizationInvitationsQuery({
    variables: {
      organizationId,
    },
    notifyOnNetworkStatusChange: true,
  });

  const isLoading = networkStatus === NetworkStatus.loading || networkStatus === NetworkStatus.refetch;

  const handleDismiss = () => {
    refetch();
  };

  return <PendingInvitationsTable invitations={organizationInvitations} isLoading={isLoading} onDismiss={handleDismiss} />;
};

export default PendingInvitationsContainer;
