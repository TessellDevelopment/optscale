import { useState } from "react";
import ActionBar from "components/ActionBar";
import EmployeesTable from "components/EmployeesTable";
import PageContentWrapper from "components/PageContentWrapper";
import TabsWrapper from "components/TabsWrapper";
import PendingInvitationsContainer from "containers/PendingInvitationsContainer";

const TABS = Object.freeze({
  EMPLOYEES: "employees",
  PENDING_INVITATIONS: "pending_invitations",
});

const Employees = ({ employees, isLoading }) => {
  const [activeTab, setActiveTab] = useState();

  const tabs = [
    {
      title: TABS.EMPLOYEES,
      dataTestId: "tab_employees",
      node: <EmployeesTable employees={employees} isLoading={isLoading} />,
    },
    {
      title: TABS.PENDING_INVITATIONS,
      dataTestId: "tab_pending_invitations",
      node: <PendingInvitationsContainer />,
    },
  ];

  return (
    <>
      <ActionBar
        data={{
          title: {
            messageId: "users",
            dataTestId: "lbl_users",
          },
        }}
      />
      <PageContentWrapper>
        <TabsWrapper
          tabsProps={{
            tabs,
            defaultTab: TABS.EMPLOYEES,
            name: "employees",
            activeTab,
            handleChange: (event, value) => {
              setActiveTab(value);
            },
          }}
        />
      </PageContentWrapper>
    </>
  );
};

export default Employees;
