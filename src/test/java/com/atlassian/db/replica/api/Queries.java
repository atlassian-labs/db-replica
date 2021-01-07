package com.atlassian.db.replica.api;

public class Queries {
    public static final String SELECT_FOR_UPDATE = "select O_S_PROPERTY_ENTRY.id, O_S_PROPERTY_ENTRY.propertytype\n" +
        "from public.propertyentry O_S_PROPERTY_ENTRY\n" +
        "where O_S_PROPERTY_ENTRY.entity_name = ? and O_S_PROPERTY_ENTRY.entity_id = ? and O_S_PROPERTY_ENTRY.property_key = ?\n" +
        "order by O_S_PROPERTY_ENTRY.id desc\n" +
        "for update";

    @SuppressWarnings({"SqlDialectInspection", "SqlNoDataSourceInspection"})
    public static final String SIMPLE_QUERY = "SELECT 1;";

    @SuppressWarnings({"SqlDialectInspection", "SqlNoDataSourceInspection"})
    public static final String LARGE_SQL_QUERY = "select \"ISSUE\".\"id\", /* com.atlassian.jira.jql.dbquery.ResolutionClauseDbQueryFactory */ case when exists (select 1\n" +
        "from \"public\".\"fieldscope\" \"FIELD_SCOPE_2\"\n" +
        "where \"FIELD_SCOPE_2\".\"project_id\" = \"ISSUE\".\"project\" and \"FIELD_SCOPE_2\".\"issue_type_id\" = \"ISSUE\".\"issuetype\" and \"FIELD_SCOPE_2\".\"field_id\" = ?) then \"ISSUE\".\"resolution\" else null end, /* com.atlassian.jira.jql.dbquery.UpdatedClauseDbQueryFactory */ \"ISSUE\".\"updated\", /* com.atlassian.jira.jql.dbquery.StringCustomFieldClauseDbQueryFactory */ (select \"CUSTOM_FIELD_VALUE_3\".\"stringvalue\"\n" +
        "from \"public\".\"customfieldvalue\" \"CUSTOM_FIELD_VALUE_3\"\n" +
        "where \"ISSUE\".\"id\" = \"CUSTOM_FIELD_VALUE_3\".\"issue\" and \"CUSTOM_FIELD_VALUE_3\".\"customfield\" = ? and exists (select 1\n" +
        "from \"public\".\"fieldscope\" \"FIELD_SCOPE_customfield_10013_5\"\n" +
        "where \"FIELD_SCOPE_customfield_10013_5\".\"field_id\" = ? and \"FIELD_SCOPE_customfield_10013_5\".\"project_id\" = \"ISSUE\".\"project\" and \"FIELD_SCOPE_customfield_10013_5\".\"issue_type_id\" = \"ISSUE\".\"issuetype\")), /* com.atlassian.jira.jql.dbquery.IssueKeyClauseDbQueryFactory */ \"ISSUE\".\"pkey\" || '-' || \"ISSUE\".\"issuenum\", /* com.atlassian.jira.jql.dbquery.ProjectClauseDbQueryFactory */ \"ISSUE\".\"project\", /* com.atlassian.jira.jql.dbquery.PriorityClauseDbQueryFactory */ case when exists (select 1\n" +
        "from \"public\".\"fieldscope\" \"FIELD_SCOPE_7\"\n" +
        "where \"FIELD_SCOPE_7\".\"project_id\" = \"ISSUE\".\"project\" and \"FIELD_SCOPE_7\".\"issue_type_id\" = \"ISSUE\".\"issuetype\" and \"FIELD_SCOPE_7\".\"field_id\" = ?) then \"ISSUE\".\"priority\" else null end, /* com.atlassian.jira.jql.dbquery.IssueParentClauseDbQueryFactory */ coalesce((select \"ISSUE_PARENT_ASSOCIATION_8\".\"parent_id\"\n" +
        "from \"public\".\"issue_parent_association\" \"ISSUE_PARENT_ASSOCIATION_8\"\n" +
        "where \"ISSUE_PARENT_ASSOCIATION_8\".\"issue_id\" = \"ISSUE\".\"id\"\n" +
        "limit ?), \"ISSUE\".\"subtask_parent_id\"), /* com.atlassian.jira.jql.dbquery.IssueTypeClauseDbQueryFactory */ \"ISSUE\".\"issuetype\", /* com.atlassian.jira.issue.customfields.searchers.MultiSelectCustomFieldClauseDbQueryFactory */ (select array_agg(cast(\"CUSTOM_FIELD_VALUE_9\".\"stringvalue\" as int8))\n" +
        "from \"public\".\"customfieldvalue\" \"CUSTOM_FIELD_VALUE_9\"\n" +
        "where \"CUSTOM_FIELD_VALUE_9\".\"issue\" = \"ISSUE\".\"id\" and \"CUSTOM_FIELD_VALUE_9\".\"customfield\" = ? and exists (select 1\n" +
        "from \"public\".\"fieldscope\" \"FIELD_SCOPE_customfield_10021_11\"\n" +
        "where \"FIELD_SCOPE_customfield_10021_11\".\"field_id\" = ? and \"FIELD_SCOPE_customfield_10021_11\".\"project_id\" = \"ISSUE\".\"project\" and \"FIELD_SCOPE_customfield_10021_11\".\"issue_type_id\" = \"ISSUE\".\"issuetype\")\n" +
        "group by \"ISSUE\".\"id\"), /* com.atlassian.greenhopper.customfield.epiclink.EpicLinkClauseDbQueryFactory */ coalesce((select \"ISSUE_LINK_12\".\"source\"\n" +
        "from \"public\".\"issuelink\" \"ISSUE_LINK_12\"\n" +
        "where \"ISSUE\".\"id\" = \"ISSUE_LINK_12\".\"destination\" and \"ISSUE_LINK_12\".\"linktype\" = ? and exists (select 1\n" +
        "from \"public\".\"fieldscope\" \"FIELD_SCOPE_customfield_10014_14\"\n" +
        "where \"FIELD_SCOPE_customfield_10014_14\".\"field_id\" = ? and \"FIELD_SCOPE_customfield_10014_14\".\"project_id\" = \"ISSUE\".\"project\" and \"FIELD_SCOPE_customfield_10014_14\".\"issue_type_id\" = \"ISSUE\".\"issuetype\")\n" +
        "order by \"ISSUE_LINK_12\".\"id\" asc\n" +
        "limit ?), ?), /* com.atlassian.greenhopper.customfield.epiclabel.EpicLabelClauseDbQueryFactory */ case when exists (select 1\n" +
        "from \"public\".\"fieldscope\" \"FIELD_SCOPE\"\n" +
        "where \"FIELD_SCOPE\".\"project_id\" = \"ISSUE\".\"project\" and \"FIELD_SCOPE\".\"issue_type_id\" = \"ISSUE\".\"issuetype\" and \"FIELD_SCOPE\".\"field_id\" = ?) then \"cfv10011\".\"stringvalue\" else null end, /* com.atlassian.jira.jql.dbquery.CreatedClauseDbQueryFactory */ \"ISSUE\".\"created\", /* com.atlassian.jira.jql.dbquery.SummaryClauseDbQueryFactory */ \"ISSUE\".\"summary\", /* com.atlassian.jira.jql.dbquery.StatusClauseDbQueryFactory */ \"ISSUE\".\"issuestatus\", /* com.atlassian.jira.jql.dbquery.FixForVersionClauseDbQueryFactory */ case when exists (select 1\n" +
        "from \"public\".\"fieldscope\" \"FIELD_SCOPE_26\"\n" +
        "where \"FIELD_SCOPE_26\".\"project_id\" = \"ISSUE\".\"project\" and \"FIELD_SCOPE_26\".\"issue_type_id\" = \"ISSUE\".\"issuetype\" and \"FIELD_SCOPE_26\".\"field_id\" = ?) then (select array_agg(\"id_24\")\n" +
        "from (select \"VERSION_22\".\"id\" as \"id_24\"\n" +
        "from \"public\".\"projectversion\" \"VERSION_22\"\n" +
        "left join \"public\".\"fixversion\" \"FIX_VERSION_21\"\n" +
        "on \"ISSUE\".\"id\" = \"FIX_VERSION_21\".\"issue\" and exists (select 1\n" +
        "from \"public\".\"fieldscope\" \"FIELD_SCOPE_fixVersions_25\"\n" +
        "where \"FIELD_SCOPE_fixVersions_25\".\"field_id\" = ? and \"FIELD_SCOPE_fixVersions_25\".\"project_id\" = \"ISSUE\".\"project\" and \"FIELD_SCOPE_fixVersions_25\".\"issue_type_id\" = \"ISSUE\".\"issuetype\")\n" +
        "where \"FIX_VERSION_21\".\"version\" = \"VERSION_22\".\"id\"\n" +
        "order by \"VERSION_22\".\"sequence\" asc nulls last) as \"version_23\") else null end, /* com.atlassian.greenhopper.customfield.sprint.SprintClauseDbQueryFactory */ (select array_agg(\"ISSUE_SPRINT_28\".\"sprint_id\")\n" +
        "from \"public\".\"issuesprint_60db71\" \"ISSUE_SPRINT_28\"\n" +
        "inner join \"public\".\"AO_60DB71_SPRINT\" \"SPRINT_27\"\n" +
        "on \"SPRINT_27\".\"ID\" = \"ISSUE_SPRINT_28\".\"sprint_id\"\n" +
        "where \"ISSUE_SPRINT_28\".\"issue_id\" = \"ISSUE\".\"effective_subtask_parent_id\" and exists (select 1\n" +
        "from \"public\".\"fieldscope\" \"FIELD_SCOPE_customfield_10020_30\"\n" +
        "where \"FIELD_SCOPE_customfield_10020_30\".\"field_id\" = ? and \"FIELD_SCOPE_customfield_10020_30\".\"project_id\" = \"ISSUE\".\"project\" and \"FIELD_SCOPE_customfield_10020_30\".\"issue_type_id\" = \"ISSUE\".\"issuetype\")\n" +
        "group by \"ISSUE_SPRINT_28\".\"issue_id\"), /* com.atlassian.jira.jql.dbquery.AssigneeClauseDbQueryFactory */ case when exists (select 1\n" +
        "from \"public\".\"fieldscope\" \"FIELD_SCOPE_32\"\n" +
        "where \"FIELD_SCOPE_32\".\"project_id\" = \"ISSUE\".\"project\" and \"FIELD_SCOPE_32\".\"issue_type_id\" = \"ISSUE\".\"issuetype\" and \"FIELD_SCOPE_32\".\"field_id\" = ?) then \"ISSUE\".\"assignee\" else null end\n" +
        "from \"public\".\"jiraissue\" \"ISSUE\"\n" +
        "left join \"public\".\"customfieldvalue\" \"cfv10011\"\n" +
        "on \"cfv10011\".\"issue\" = \"ISSUE\".\"id\" and \"cfv10011\".\"customfield\" = ?\n" +
        "where (/* com.atlassian.jira.jql.dbquery.UpdatedClauseDbQueryFactory */ (\"ISSUE\".\"updated\" >= ? and true) or /* com.atlassian.jira.jql.dbquery.StatusCategoryClauseDbQueryFactory */ (not exists (select 1\n" +
        "from \"public\".\"issuestatus\" \"STATUS_37\"\n" +
        "where (\"STATUS_37\".\"statuscategory\" = ? or \"STATUS_37\".\"statuscategory\" is null) and \"ISSUE\".\"issuestatus\" = \"STATUS_37\".\"id\") and exists (select 1\n" +
        "from \"public\".\"issuestatus\" \"STATUS_37\"\n" +
        "where true and \"ISSUE\".\"issuestatus\" = \"STATUS_37\".\"id\") and true)) and (/* com.atlassian.jira.jql.dbquery.StatusClauseDbQueryFactory */ (\"ISSUE\".\"issuestatus\" in (?, ?, ?, ?)) and /* com.atlassian.jira.jql.dbquery.ProjectClauseDbQueryFactory */ (\"ISSUE\".\"project\" = ?) and /* com.atlassian.jira.jql.dbquery.FixForVersionClauseDbQueryFactory */ (not exists (select 1\n" +
        "from \"public\".\"fixversion\" \"FIX_VERSION_38\"\n" +
        "where \"FIX_VERSION_38\".\"version\" is not null and \"ISSUE\".\"id\" = \"FIX_VERSION_38\".\"issue\") and exists (select 1\n" +
        "from \"public\".\"fieldscope\" \"FIELD_SCOPE_fixVersions_39\"\n" +
        "where \"FIELD_SCOPE_fixVersions_39\".\"field_id\" = ? and \"FIELD_SCOPE_fixVersions_39\".\"project_id\" = \"ISSUE\".\"project\" and \"FIELD_SCOPE_fixVersions_39\".\"issue_type_id\" = \"ISSUE\".\"issuetype\"))) and ((\"ISSUE\".\"project\" in (VALUES (?)) or false and \"ISSUE\".\"reporter\" = ? or false and \"ISSUE\".\"assignee\" = ? or exists (select 1\n" +
        "from \"public\".\"jiraissue\" \"ISSUE_43\"\n" +
        "join \"public\".\"project\" \"PROJECT_41\"\n" +
        "on \"PROJECT_41\".\"id\" = \"ISSUE_43\".\"project\"\n" +
        "join \"public\".\"schemepermissions\" \"SCHEME_PERMISSIONS_40\"\n" +
        "on \"PROJECT_41\".\"permissionscheme\" = \"SCHEME_PERMISSIONS_40\".\"scheme\" and \"SCHEME_PERMISSIONS_40\".\"permission_key\" = ? and \"SCHEME_PERMISSIONS_40\".\"perm_type\" = ?\n" +
        "join \"public\".\"customfieldvalue\" \"CUSTOM_FIELD_VALUE_42\"\n" +
        "on ? || \"CUSTOM_FIELD_VALUE_42\".\"customfield\" = \"SCHEME_PERMISSIONS_40\".\"perm_parameter\" and \"CUSTOM_FIELD_VALUE_42\".\"issue\" = \"ISSUE_43\".\"id\"\n" +
        "where \"PROJECT_41\".\"status\" = ? and \"ISSUE_43\".\"id\" = \"ISSUE\".\"id\" and \"CUSTOM_FIELD_VALUE_42\".\"stringvalue\" = ?) or (exists (select 1\n" +
        "from \"public\".\"jiraissue\" \"ISSUE_48\"\n" +
        "join \"public\".\"project\" \"PROJECT_46\"\n" +
        "on \"PROJECT_46\".\"id\" = \"ISSUE_48\".\"project\"\n" +
        "join \"public\".\"schemepermissions\" \"SCHEME_PERMISSIONS_45\"\n" +
        "on \"PROJECT_46\".\"permissionscheme\" = \"SCHEME_PERMISSIONS_45\".\"scheme\" and \"SCHEME_PERMISSIONS_45\".\"permission_key\" = ? and \"SCHEME_PERMISSIONS_45\".\"perm_type\" = ?\n" +
        "join \"public\".\"customfieldvalue\" \"CUSTOM_FIELD_VALUE_47\"\n" +
        "on ? || \"CUSTOM_FIELD_VALUE_47\".\"customfield\" = \"SCHEME_PERMISSIONS_45\".\"perm_parameter\" and \"CUSTOM_FIELD_VALUE_47\".\"issue\" = \"ISSUE_48\".\"id\"\n" +
        "join \"public\".\"customfield\" \"CUSTOM_FIELD_44\"\n" +
        "on \"CUSTOM_FIELD_44\".\"id\" = \"CUSTOM_FIELD_VALUE_47\".\"customfield\"\n" +
        "where \"PROJECT_46\".\"status\" = ? and \"ISSUE_48\".\"id\" = \"ISSUE\".\"id\" and lower(\"CUSTOM_FIELD_VALUE_47\".\"stringvalue\") in (select \"MEMBERSHIP_49\".\"lower_parent_name\"\n" +
        "from \"public\".\"cwd_membership\" \"MEMBERSHIP_49\"\n" +
        "where \"MEMBERSHIP_49\".\"lower_child_name\" = ? and \"MEMBERSHIP_49\".\"directory_id\" = ? and \"MEMBERSHIP_49\".\"membership_type\" = ?) and \"CUSTOM_FIELD_44\".\"customfieldtypekey\" in (?, ?)) or exists (select 1\n" +
        "from \"public\".\"jiraissue\" \"ISSUE_55\"\n" +
        "join \"public\".\"project\" \"PROJECT_53\"\n" +
        "on \"PROJECT_53\".\"id\" = \"ISSUE_55\".\"project\"\n" +
        "join \"public\".\"schemepermissions\" \"SCHEME_PERMISSIONS_52\"\n" +
        "on \"PROJECT_53\".\"permissionscheme\" = \"SCHEME_PERMISSIONS_52\".\"scheme\" and \"SCHEME_PERMISSIONS_52\".\"permission_key\" = ? and \"SCHEME_PERMISSIONS_52\".\"perm_type\" = ?\n" +
        "join \"public\".\"customfieldvalue\" \"CUSTOM_FIELD_VALUE_54\"\n" +
        "on ? || \"CUSTOM_FIELD_VALUE_54\".\"customfield\" = \"SCHEME_PERMISSIONS_52\".\"perm_parameter\" and \"CUSTOM_FIELD_VALUE_54\".\"issue\" = \"ISSUE_55\".\"id\"\n" +
        "join \"public\".\"customfield\" \"CUSTOM_FIELD_50\"\n" +
        "on \"CUSTOM_FIELD_50\".\"id\" = \"CUSTOM_FIELD_VALUE_54\".\"customfield\"\n" +
        "join \"public\".\"customfieldoption\" \"CUSTOM_FIELD_OPTION_51\"\n" +
        "on cast(\"CUSTOM_FIELD_OPTION_51\".\"id\" as varchar) = \"CUSTOM_FIELD_VALUE_54\".\"stringvalue\"\n" +
        "where \"PROJECT_53\".\"status\" = ? and \"ISSUE_55\".\"id\" = \"ISSUE\".\"id\" and lower(\"CUSTOM_FIELD_OPTION_51\".\"customvalue\") in (select \"MEMBERSHIP_56\".\"lower_parent_name\"\n" +
        "from \"public\".\"cwd_membership\" \"MEMBERSHIP_56\"\n" +
        "where \"MEMBERSHIP_56\".\"lower_child_name\" = ? and \"MEMBERSHIP_56\".\"directory_id\" = ? and \"MEMBERSHIP_56\".\"membership_type\" = ?) and \"CUSTOM_FIELD_50\".\"customfieldtypekey\" in (?, ?)))) and (\"ISSUE\".\"security\" is null or (exists (select 1\n" +
        "from \"public\".\"schemeissuesecurities\" \"SCHEME_ISSUE_SECURITIES_57\"\n" +
        "where \"ISSUE\".\"security\" = \"SCHEME_ISSUE_SECURITIES_57\".\"security\" and (\"SCHEME_ISSUE_SECURITIES_57\".\"sec_type\" = ? and \"SCHEME_ISSUE_SECURITIES_57\".\"sec_parameter\" = ? or (\"SCHEME_ISSUE_SECURITIES_57\".\"sec_type\" = ? and lower(\"SCHEME_ISSUE_SECURITIES_57\".\"sec_parameter\") in (select \"MEMBERSHIP_58\".\"lower_parent_name\"\n" +
        "from \"public\".\"cwd_membership\" \"MEMBERSHIP_58\"\n" +
        "where \"MEMBERSHIP_58\".\"lower_child_name\" = ? and \"MEMBERSHIP_58\".\"directory_id\" = ? and \"MEMBERSHIP_58\".\"membership_type\" = ?) or \"SCHEME_ISSUE_SECURITIES_57\".\"sec_type\" = ? and \"SCHEME_ISSUE_SECURITIES_57\".\"sec_parameter\" is null) or (\"SCHEME_ISSUE_SECURITIES_57\".\"sec_type\" = ? and \"SCHEME_ISSUE_SECURITIES_57\".\"sec_parameter\" in (select \"LICENSE_ROLE_GROUP_59\".\"license_role_name\"\n" +
        "from \"public\".\"licenserolesgroup\" \"LICENSE_ROLE_GROUP_59\"\n" +
        "where lower(\"LICENSE_ROLE_GROUP_59\".\"group_id\") in (select \"MEMBERSHIP_60\".\"lower_parent_name\"\n" +
        "from \"public\".\"cwd_membership\" \"MEMBERSHIP_60\"\n" +
        "where \"MEMBERSHIP_60\".\"lower_child_name\" = ? and \"MEMBERSHIP_60\".\"directory_id\" = ? and \"MEMBERSHIP_60\".\"membership_type\" = ?)) or \"SCHEME_ISSUE_SECURITIES_57\".\"sec_type\" = ? and (\"SCHEME_ISSUE_SECURITIES_57\".\"sec_parameter\" is null or length(\"SCHEME_ISSUE_SECURITIES_57\".\"sec_parameter\") = 0 or \"SCHEME_ISSUE_SECURITIES_57\".\"sec_parameter\" = ?)))) or exists (select 1\n" +
        "from \"public\".\"schemeissuesecurities\" \"SCHEME_ISSUE_SECURITIES_61\"\n" +
        "join \"public\".\"projectroleactor\" \"PROJECT_ROLE_ACTOR_62\"\n" +
        "on \"SCHEME_ISSUE_SECURITIES_61\".\"sec_parameter\" = cast(\"PROJECT_ROLE_ACTOR_62\".\"projectroleid\" as varchar)\n" +
        "where \"SCHEME_ISSUE_SECURITIES_61\".\"sec_type\" = ? and (\"PROJECT_ROLE_ACTOR_62\".\"roletype\" = ? and lower(\"PROJECT_ROLE_ACTOR_62\".\"roletypeparameter\") in (select \"MEMBERSHIP_63\".\"lower_parent_name\"\n" +
        "from \"public\".\"cwd_membership\" \"MEMBERSHIP_63\"\n" +
        "where \"MEMBERSHIP_63\".\"lower_child_name\" = ? and \"MEMBERSHIP_63\".\"directory_id\" = ? and \"MEMBERSHIP_63\".\"membership_type\" = ?) or \"PROJECT_ROLE_ACTOR_62\".\"roletype\" = ? and \"PROJECT_ROLE_ACTOR_62\".\"roletypeparameter\" = ?) and \"PROJECT_ROLE_ACTOR_62\".\"pid\" = \"ISSUE\".\"project\" and \"ISSUE\".\"security\" = \"SCHEME_ISSUE_SECURITIES_61\".\"security\") or \"ISSUE\".\"reporter\" = ? and \"ISSUE\".\"security\" in (select \"SCHEME_ISSUE_SECURITIES_64\".\"security\"\n" +
        "from \"public\".\"schemeissuesecurities\" \"SCHEME_ISSUE_SECURITIES_64\"\n" +
        "where \"SCHEME_ISSUE_SECURITIES_64\".\"sec_type\" = ?) or \"ISSUE\".\"assignee\" = ? and \"ISSUE\".\"security\" in (select \"SCHEME_ISSUE_SECURITIES_65\".\"security\"\n" +
        "from \"public\".\"schemeissuesecurities\" \"SCHEME_ISSUE_SECURITIES_65\"\n" +
        "where \"SCHEME_ISSUE_SECURITIES_65\".\"sec_type\" = ?) or exists (select 1\n" +
        "from \"public\".\"project\" \"PROJECT_66\"\n" +
        "where \"PROJECT_66\".\"lead\" = ? and \"ISSUE\".\"security\" in (select \"SCHEME_ISSUE_SECURITIES_67\".\"security\"\n" +
        "from \"public\".\"schemeissuesecurities\" \"SCHEME_ISSUE_SECURITIES_67\"\n" +
        "where \"SCHEME_ISSUE_SECURITIES_67\".\"sec_type\" = ?) and \"ISSUE\".\"project\" = \"PROJECT_66\".\"id\") or exists (select 1\n" +
        "from \"public\".\"customfieldvalue\" \"CUSTOM_FIELD_VALUE_68\"\n" +
        "join \"public\".\"schemeissuesecurities\" \"SCHEME_ISSUE_SECURITIES_69\"\n" +
        "on \"SCHEME_ISSUE_SECURITIES_69\".\"sec_type\" = ? and \"SCHEME_ISSUE_SECURITIES_69\".\"sec_parameter\" = ? || \"CUSTOM_FIELD_VALUE_68\".\"customfield\"\n" +
        "where \"CUSTOM_FIELD_VALUE_68\".\"stringvalue\" = ? and \"CUSTOM_FIELD_VALUE_68\".\"issue\" = \"ISSUE\".\"id\" and \"SCHEME_ISSUE_SECURITIES_69\".\"security\" = \"ISSUE\".\"security\") or (exists (select 1\n" +
        "from \"public\".\"customfieldvalue\" \"CUSTOM_FIELD_VALUE_71\"\n" +
        "join \"public\".\"customfield\" \"CUSTOM_FIELD_70\"\n" +
        "on \"CUSTOM_FIELD_70\".\"id\" = \"CUSTOM_FIELD_VALUE_71\".\"customfield\"\n" +
        "join \"public\".\"schemeissuesecurities\" \"SCHEME_ISSUE_SECURITIES_72\"\n" +
        "on \"SCHEME_ISSUE_SECURITIES_72\".\"sec_type\" = ? and \"SCHEME_ISSUE_SECURITIES_72\".\"sec_parameter\" = ? || \"CUSTOM_FIELD_VALUE_71\".\"customfield\"\n" +
        "where lower(\"CUSTOM_FIELD_VALUE_71\".\"stringvalue\") in (select \"MEMBERSHIP_73\".\"lower_parent_name\"\n" +
        "from \"public\".\"cwd_membership\" \"MEMBERSHIP_73\"\n" +
        "where \"MEMBERSHIP_73\".\"lower_child_name\" = ? and \"MEMBERSHIP_73\".\"directory_id\" = ? and \"MEMBERSHIP_73\".\"membership_type\" = ?) and \"CUSTOM_FIELD_70\".\"customfieldtypekey\" in (?, ?) and \"CUSTOM_FIELD_VALUE_71\".\"issue\" = \"ISSUE\".\"id\" and \"SCHEME_ISSUE_SECURITIES_72\".\"security\" = \"ISSUE\".\"security\") or exists (select 1\n" +
        "from \"public\".\"customfieldvalue\" \"CUSTOM_FIELD_VALUE_75\"\n" +
        "join \"public\".\"customfieldoption\" \"CUSTOM_FIELD_OPTION_76\"\n" +
        "on cast(\"CUSTOM_FIELD_OPTION_76\".\"id\" as varchar) = \"CUSTOM_FIELD_VALUE_75\".\"stringvalue\"\n" +
        "join \"public\".\"customfield\" \"CUSTOM_FIELD_74\"\n" +
        "on \"CUSTOM_FIELD_74\".\"id\" = \"CUSTOM_FIELD_OPTION_76\".\"customfield\"\n" +
        "join \"public\".\"schemeissuesecurities\" \"SCHEME_ISSUE_SECURITIES_77\"\n" +
        "on \"SCHEME_ISSUE_SECURITIES_77\".\"sec_type\" = ? and \"SCHEME_ISSUE_SECURITIES_77\".\"sec_parameter\" = ? || \"CUSTOM_FIELD_VALUE_75\".\"customfield\"\n" +
        "where cast(\"CUSTOM_FIELD_OPTION_76\".\"id\" as varchar) = \"CUSTOM_FIELD_VALUE_75\".\"stringvalue\" and lower(\"CUSTOM_FIELD_OPTION_76\".\"customvalue\") in (select \"MEMBERSHIP_78\".\"lower_parent_name\"\n" +
        "from \"public\".\"cwd_membership\" \"MEMBERSHIP_78\"\n" +
        "where \"MEMBERSHIP_78\".\"lower_child_name\" = ? and \"MEMBERSHIP_78\".\"directory_id\" = ? and \"MEMBERSHIP_78\".\"membership_type\" = ?) and \"CUSTOM_FIELD_74\".\"customfieldtypekey\" in (?, ?) and \"CUSTOM_FIELD_VALUE_75\".\"issue\" = \"ISSUE\".\"id\" and \"SCHEME_ISSUE_SECURITIES_77\".\"security\" = \"ISSUE\".\"security\"))) and exists (select 1\n" +
        "from \"public\".\"project\" \"PROJECT_79\"\n" +
        "where \"PROJECT_79\".\"id\" = \"ISSUE\".\"project\" and \"PROJECT_79\".\"issuesecurityscheme\" = (select \"SCHEME_ISSUE_SECURITY_LEVELS\".\"scheme\"\n" +
        "from \"public\".\"schemeissuesecuritylevels\" \"SCHEME_ISSUE_SECURITY_LEVELS\"\n" +
        "where \"SCHEME_ISSUE_SECURITY_LEVELS\".\"id\" = \"ISSUE\".\"security\"))))\n" +
        "order by case when exists (select 1\n" +
        "from \"public\".\"fieldscope\" \"FIELD_SCOPE\"\n" +
        "where \"FIELD_SCOPE\".\"project_id\" = \"ISSUE\".\"project\" and \"FIELD_SCOPE\".\"issue_type_id\" = \"ISSUE\".\"issuetype\" and \"FIELD_SCOPE\".\"field_id\" = ?) then (select max(\"PARENT_LEXORANK_35\".\"RANK\")\n" +
        "from \"public\".\"AO_60DB71_LEXORANK\" \"PARENT_LEXORANK_35\"\n" +
        "where \"PARENT_LEXORANK_35\".\"ISSUE_ID\" = \"ISSUE\".\"effective_subtask_parent_id\" and \"PARENT_LEXORANK_35\".\"FIELD_ID\" = ?) else null end asc nulls last, case when exists (select 1\n" +
        "from \"public\".\"fieldscope\" \"FIELD_SCOPE\"\n" +
        "where \"FIELD_SCOPE\".\"project_id\" = \"ISSUE\".\"project\" and \"FIELD_SCOPE\".\"issue_type_id\" = \"ISSUE\".\"issuetype\" and \"FIELD_SCOPE\".\"field_id\" = ?) then (select max(\"CHILD_LEXORANK_36\".\"RANK\")\n" +
        "from \"public\".\"AO_60DB71_LEXORANK\" \"CHILD_LEXORANK_36\"\n" +
        "where \"ISSUE\".\"subtask_parent_id\" is not null and \"CHILD_LEXORANK_36\".\"ISSUE_ID\" = \"ISSUE\".\"id\" and \"CHILD_LEXORANK_36\".\"FIELD_ID\" = ?) else null end asc nulls first, \"ISSUE\".\"pkey\" desc nulls first, \"ISSUE\".\"issuenum\" desc nulls first\n" +
        "offset ?, update \"public\".\"jiraworkflows\" \"WORKFLOW\"\n" +
        "set \"descriptor\" = translate(descriptor, U&'\\0008', '')\n" +
        "where \"WORKFLOW\".\"descriptor\" like ?" +
        "left join \"public\".\"project\" \"PROJECT\"";
}
