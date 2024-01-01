# Prefect

> [!NOTE]
> Examples that start with the `cli` command indicate that the command would be executed inside of the Docker container. `cli` can also be accessed outside of the Docker container at `./scripts/cli.sh`. The arguments and options are the same.

## Dashboard

The Prefect server provides a dashboard of flows, deployments and more. To view it:

1. Start the Prefect containers if they are not running already:

    ```shell
    docker compose up -d prefect-postgres prefect-server prefect-worker prefect-cli
    ```

2. Open [http://localhost:29020](http://localhost:29020).

## Flows and tasks

Flows exist as Python functions and flow entities. Python functions are stored in `./projects/<PROJECT_NAME>/flows`, and the flow entities are stored in the Prefect database.

Flow entities appear on the Prefect dashboard under [Flows](http://localhost:29020/flows) and in the CLI at `prefect flows ls`. Flows that have not been deployed won't be listed.

The Python files in `./projects/<PROJECT_NAME>/flows` contain 1 main flow function, optionally subflows, and 1 or more task functions that are used in the flow.

Further reading:

- the Prefect docs for [flows](https://docs.prefect.io/latest/concepts/flows/) and [tasks](https://docs.prefect.io/latest/concepts/tasks/)
- [naming conventions for flows and deployments](#naming-conventions-for-flows-and-deployments)

## Deployments

Deployments of flows exist as configuration files and deployment entities. The configuration is stored in `./projects/<PROJECT_NAME>/prefect.yaml`, and the deployment entities are stored in the Prefect database.

Deployment entities appear on the Prefect dashboard under [Deployments](http://localhost:29020/deployments) and in the CLI at `prefect deployments ls`. Deployments that exist only in configuration won't be listed.

Further reading:

- the Prefect docs for [deployments](https://docs.prefect.io/latest/concepts/deployments/) and [schedules](https://docs.prefect.io/latest/concepts/schedules/#creating-schedules-through-a-prefectyaml-files-deployments-schedule-section)
- [naming conventions for flows and deployments](#naming-conventions-for-flows-and-deployments)

### Create a deployment

For example, here are the steps to create a deployment in the `dw_tutorial` project:

1. Open [./projects/dw_tutorial/prefect.yaml](../projects/dw_tutorial/prefect.yaml#L44) and go to the commented section `Instructions for new deployments`.

2. Copy and append the template to the `deployments` list.

3. Replace `<FLOW_NAME>` with the flow name.

4. Update the parameters, schedule and other attributes.

5. Run the deploy command:

    ```shell
    cli prefect deploy -d DEPLOYMENT_NAME
    ```

    `DEPLOYMENT_NAME` is the `name` value in the `deployments` list.

    By default, new deployments are active. To create a paused deployment, add the `--pause` option:

    ```shell
    cli prefect deploy -d DEPLOYMENT_NAME --pause
    ```

Available commands:

| Description                                 | Command                                                               |
|---------------------------------------------|-----------------------------------------------------------------------|
| Deploy all flows of 1 project               | `cli prefect deploy -p PROJECT_NAME`                                  |
| Deploy all flows of multiple projects       | `cli prefect deploy -p PROJECT_NAME -p PROJECT_NAME`                  |
| Deploy 1 flow                               | `cli prefect deploy -d DEPLOYMENT_NAME`                               |
| Deploy multiple flows                       | `cli prefect deploy -d DEPLOYMENT_NAME -d DEPLOYMENT_NAME`            |

### Pause a deployment

| Description                                 | Command                                                               |
|---------------------------------------------|-----------------------------------------------------------------------|
| Pause all deployments of 1 project          | `cli prefect deployment pause -p PROJECT_NAME`                        |
| Pause all deployments of multiple projects  | `cli prefect deployment pause -p PROJECT_NAME -p PROJECT_NAME`        |
| Pause 1 deployment                          | `cli prefect deployment pause -d DEPLOYMENT_NAME`                     |
| Pause multiple deployments                  | `cli prefect deployment pause -d DEPLOYMENT_NAME -d DEPLOYMENT_NAME`  |

### Resume a deployment

| Description                                 | Command                                                               |
|---------------------------------------------|-----------------------------------------------------------------------|
| Resume all deployments of 1 project         | `cli prefect deployment resume -p PROJECT_NAME`                       |
| Resume all deployments of multiple projects | `cli prefect deployment resume -p PROJECT_NAME -p PROJECT_NAME`       |
| Resume 1 deployment                         | `cli prefect deployment resume -d DEPLOYMENT_NAME`                    |
| Resume multiple deployments                 | `cli prefect deployment resume -d DEPLOYMENT_NAME -d DEPLOYMENT_NAME` |

### Delete a deployment

To delete a deployment:

1. Run the `delete` command to delete the deployment entity from the Prefect database:

    ```shell
    cli prefect deployment delete -d DEPLOYMENT_NAME
    ```

2. Remove the deployment from the `deployments` list in `./projects/<PROJECT_NAME>/prefect.yaml`.

Available commands:

| Description                                 | Command                                                               |
|---------------------------------------------|-----------------------------------------------------------------------|
| Delete all deployments of 1 project         | `cli prefect deployment delete -p PROJECT_NAME`                       |
| Delete all deployments of multiple projects | `cli prefect deployment delete -p PROJECT_NAME -p PROJECT_NAME`       |
| Delete 1 deployment                         | `cli prefect deployment delete -d DEPLOYMENT_NAME`                    |
| Delete multiple deployments                 | `cli prefect deployment delete -d DEPLOYMENT_NAME -d DEPLOYMENT_NAME` |

## Naming conventions for flows and deployments

Conventions for `./projects/<PROJECT_NAME>/flows` and `./projects/<PROJECT_NAME>/prefect.yaml`:

- flow file name: `<FLOW_NAME>.py` (has no suffix)
- flow function name: `<FLOW_NAME>_flow` (has suffix)
- flow function `@flow` decorator `name` parameter: `<PROJECT_NAME>__<FLOW_NAME>_flow` (has suffix). Must be unique across all projects.
- deployment name: `<PROJECT_NAME>__<FLOW_NAME>` (has no suffix). Must be unique across all projects.
- deployment entrypoint: `flows/<FLOW_NAME>.py:<FLOW_NAME>_flow`

For example, if the project name = `farm` and the flow name = `harvest`, then:

- flow file name: `harvest.py`
- flow function name: `harvest_flow`
- flow function `@flow` decorator `name` parameter: `farm__harvest_flow`
- deployment name: `farm__harvest`
- deployment entrypoint: `flows/harvest.py:harvest_flow`

If the flow function has parameters and if there are multiple deployments of the same flow, i.e. parameterized, then include the parameter:

- deployment name: `<PROJECT_NAME>__<FLOW_NAME>__<PARAMETER>`

`<PARAMETER>` can be the value of the parameter or a short name that summarizes the specifics of the deployment, e.g. if the parameter value is a list of values.

For example, if the project name = `farm`, the flow name = `harvest`, the flow function has a `season` parameter, and there are 2 deployments of the same flow for `winter` and `summer`, then:

Deployment 1:

- deployment name: `farm__harvest__winter`
- deployment parameters: `{"season": "winter"}`

Deployment 2:

- deployment name: `farm__harvest__summer`
- deployment parameters: `{"season": "summer"}`

## Resources

- [Prefect docs](https://docs.prefect.io)

## Next step

[Read the ops guide](ops.md).
