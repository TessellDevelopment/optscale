import { Typography } from "@mui/material";
import { FormattedMessage } from "react-intl";
import ButtonLoader from "components/ButtonLoader";
import FormButtonsWrapper from "components/FormButtonsWrapper";
import { useDismissInvitationMutation } from "graphql/__generated__/hooks/restapi";

const DismissInvitationContainer = ({ invitationId, email, onSuccess, closeSideModal }) => {
  const [dismissInvitation, { loading }] = useDismissInvitationMutation();

  const onDismiss = () => {
    dismissInvitation({
      variables: { invitationId },
    })
      .then(() => {
        if (typeof onSuccess === "function") {
          onSuccess();
        }
        closeSideModal();
      })
      .catch((error) => {
        console.error("Error dismissing invitation:", error);
        // Still close the modal even on error so user isn't stuck
        closeSideModal();
      });
  };

  return (
    <>
      <Typography paragraph>
        <FormattedMessage
          id="dismissInvitationQuestion"
          values={{
            email: <strong>{email}</strong>,
          }}
        />
      </Typography>
      <FormButtonsWrapper>
        <ButtonLoader
          dataTestId="btn_dismiss"
          messageId="dismiss"
          color="error"
          variant="contained"
          onClick={onDismiss}
          isLoading={loading}
        />
        <ButtonLoader messageId="cancel" dataTestId="btn_cancel" onClick={closeSideModal} />
      </FormButtonsWrapper>
    </>
  );
};

export default DismissInvitationContainer;
