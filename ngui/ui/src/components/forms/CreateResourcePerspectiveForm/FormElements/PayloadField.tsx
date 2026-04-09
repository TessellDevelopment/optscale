import { useEffect } from "react";
import { FormHelperText } from "@mui/material";
import { useFormContext } from "react-hook-form";
import { useIntl } from "react-intl";
import { validatePerspectiveSchema } from "utils/validation";
import { FIELD_NAMES } from "../constants";

const FIELD_NAME = FIELD_NAMES.PAYLOAD;

const PayloadField = () => {
  const {
    register,
    unregister,
    formState: { errors },
  } = useFormContext();

  const intl = useIntl();

  useEffect(() => {
    register(FIELD_NAME, {
      validate: {
        isFormatValid: (data) => {
          const [isValid, errors] = validatePerspectiveSchema(data);

          if (!isValid) {
            console.error("❌ Perspective schema validation failed:", {
              data,
              errors,
            });
          } else {
            console.log("✅ Perspective schema validation passed");
          }

          return isValid ? true : intl.formatMessage({ id: "incorrectDataFormat" });
        },
      },
    });

    return () => unregister(FIELD_NAME);
  }, [intl, register, unregister]);

  return errors?.[FIELD_NAMES.PAYLOAD]?.message && <FormHelperText error>{errors?.[FIELD_NAME]?.message}</FormHelperText>;
};

export default PayloadField;
