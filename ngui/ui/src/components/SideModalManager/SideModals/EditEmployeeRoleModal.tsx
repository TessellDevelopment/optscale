import EditEmployeeRoleContainer from "containers/EditEmployeeRoleContainer";
import BaseSideModal from "./BaseSideModal";

class EditEmployeeRoleModal extends BaseSideModal {
  headerProps = {
    messageId: "editEmployeeRoleTitle",
    color: "primary",
    dataTestIds: {
      title: "lbl_edit_employee_role",
      closeButton: "btn_close",
    },
  };

  dataTestId = "smodal_edit_employee_role";

  get content() {
    return (
      <EditEmployeeRoleContainer
        employee={this.payload?.employee}
        organizationId={this.payload?.organizationId}
        closeSideModal={this.closeSideModal}
      />
    );
  }
}

export default EditEmployeeRoleModal;
