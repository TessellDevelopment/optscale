import CreateResourcePerspectiveForm from "components/forms/CreateResourcePerspectiveForm";
import {
  OrganizationPerspectivesDocument,
  useUpdateOrganizationPerspectivesMutation,
} from "graphql/__generated__/hooks/restapi";
import { useOrganizationPerspectives } from "hooks/coreData/useOrganizationPerspectives";
import { useOrganizationInfo } from "hooks/useOrganizationInfo";

const CreateResourcePerspectiveContainer = ({
  breakdownBy,
  breakdownData,
  onSuccess,
  onCancel,
  filterValues,
  appliedFilters,
}) => {
  const { organizationId } = useOrganizationInfo();

  const { allPerspectives } = useOrganizationPerspectives();

  const [updateOrganizationPerspectives, { loading }] = useUpdateOrganizationPerspectivesMutation({
    update: (cache, { data }) => {
      cache.writeQuery({
        query: OrganizationPerspectivesDocument,
        variables: { organizationId },
        data: {
          organizationPerspectives: data.updateOrganizationPerspectives,
        },
      });
    },
  });

  const onSubmit = (data) => {
    console.log("📝 Attempting to save perspective:", {
      name: data.name,
      payload: data.payload,
      organizationId,
      allPerspectives,
    });

    return updateOrganizationPerspectives({
      variables: {
        organizationId,
        value: {
          ...allPerspectives,
          [data.name]: data.payload,
        },
      },
    })
      .then((result) => {
        console.log("✅ Save successful:", result);
        onSuccess();
      })
      .catch((error) => {
        console.error("❌ Save failed:", error);
        console.error("Error details:", {
          message: error.message,
          graphQLErrors: error.graphQLErrors,
          networkError: error.networkError,
          extraInfo: error.extraInfo,
        });
        throw error;
      });
  };

  return (
    <CreateResourcePerspectiveForm
      onSubmit={onSubmit}
      isLoading={loading}
      breakdownBy={breakdownBy}
      breakdownData={breakdownData}
      perspectiveNames={Object.keys(allPerspectives)}
      onCancel={onCancel}
      filterValues={filterValues}
      appliedFilters={appliedFilters}
    />
  );
};

export default CreateResourcePerspectiveContainer;
