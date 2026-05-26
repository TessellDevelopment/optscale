import DismissInvitationContainer from "containers/DismissInvitationContainer";
import BaseSideModal from "./BaseSideModal";

class DismissInvitationModal extends BaseSideModal {
  headerProps = {
    messageId: "dismissInvitation",
    color: "error",
    dataTestIds: {
      title: "lbl_dismiss_invitation",
      closeButton: "btn_close",
    },
  };

  dataTestId = "smodal_dismiss_invitation";

  get content() {
    return (
      <DismissInvitationContainer
        invitationId={this.payload?.invitationId}
        email={this.payload?.email}
        onSuccess={this.payload?.onSuccess}
        closeSideModal={this.closeSideModal}
      />
    );
  }
}

export default DismissInvitationModal;
