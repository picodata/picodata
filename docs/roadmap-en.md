# Picodata roadmap
This document describes the estimated development plan for the Picodata product for the upcoming quarters. In each case (for each major release), the goal is to achieve the stated functionality in the plan.
The Picodata Roadmap is aligned with the Release Policy to provide greater clarity on the development process and inform stakeholders about the plans and priorities of the Picodata development team.
A new versioning format is introduced, based on calendar versioning (CalVer).

So, the current release of Picodata is designated as 22.07.0, where:

22 - the number of the year in which the release took place;

07 - the number of the month in which the release took place;

0 is the number of the minor version, reflecting the presence and/or number of improvements made as part of the major release.

The release policy of Picodata LLC provides for the release of new major versions of Picodata 4 times a year, approximately once a quarter.
The following functionality is currently planned for future releases of Picodata:

Q4-2022 (22.10.0)
Adding the vshard module, which will ensure the distribution of data segments between different replicasets. Support for distributing data across cluster nodes in accordance with specified criteria. Access to data from any cluster node.

Q1-2023 (23.01.0)
Bringing the Picodata API to a functional state, providing the ability to create and delete tables (spaces) in the DBMS using the Picodata API.

Q2-2023 (23.04.0)
Implementation of an automatic data balancer in a cluster that moves data from more full cluster nodes to less full ones in order to maintain an even distribution of data across cluster nodes.
Implementation of partial support for the SQL:2016 standard within the entire cluster (support for distributed SQL), including elements from subsections: E011. Numeric data types, E011-05. Numerical Comparisons, E021. Character string types.

Q3-2023 (23.07.0)
Implementation of a distributed mechanism for managing the DBMS data schema (tables, stored procedures, users, privileges), which guarantees an identical data schema on all cluster nodes.
Cluster-wide extension of SQL:2016 support (distributed SQL support), including items from subsections: E031. Identifiers, E051. Base Request Specification, E061. Basic Predicates and Search Conditions, E071. Basic query expressions, E101. Basic data processing.

Q4-2023 (23.10.0)
Integration of Tarantool-Rust-module into the main Picodata application.
Implementation of the mechanism for executing tasks in a cluster in the semantics “exactly once”, “no more than once”.
Implementation of the mechanism of roles - the distribution of computing and application tasks among the nodes of the cluster based on their purpose (role), which provides centralized management of the program executed by the cluster.
Cluster-wide SQL:2016 support extension (distributed SQL support), including items from subsections: F041. Basic join of tables, F471. Subquery Scalar Values, T631. IN predicate with one list element.

