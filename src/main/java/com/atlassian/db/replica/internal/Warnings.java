package com.atlassian.db.replica.internal;

import java.sql.SQLWarning;

public final class Warnings {
    private SQLWarning warning;

    public void saveWarning(SQLWarning warning) {
        if (warning == null || isLastWarning(warning)) {
            return;
        }
        if (this.warning == null) {
            this.warning = warning;
        } else {
            this.warning.setNextWarning(warning);
        }
    }

    public SQLWarning getWarning() {
        return warning;
    }

    public void clear() {
        this.warning = null;
    }

    private boolean isLastWarning(SQLWarning warning) {
        if (this.warning == null) {
            return false;
        }
        SQLWarning lastWarning = this.warning;
        for (int i = 0; i < 100; i++) {
            if (lastWarning.getNextWarning() == null) {
                return lastWarning.equals(warning);
            } else
                lastWarning = lastWarning.getNextWarning();
        }
        return true;
    }
}
